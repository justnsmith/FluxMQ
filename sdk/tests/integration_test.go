package tests

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"fluxmq/sdk/fluxmq"
)

// brokerAddr is set by TestMain once the broker is started.
var brokerAddr string

// brokerBin is the path to the broker binary relative to the test file.
const brokerBin = "../../build/fluxmq"

// TestMain starts the broker binary, runs all tests, then kills the broker.
func TestMain(m *testing.M) {
	if _, err := os.Stat(brokerBin); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "broker binary not found at %s; skipping integration tests\n", brokerBin)
		os.Exit(0)
	}

	addr, cleanup := startBrokerGlobal()
	brokerAddr = addr

	code := m.Run()
	cleanup()
	os.Exit(code)
}

// startBrokerGlobal starts a broker process and returns its address and a cleanup function.
// Used by TestMain (not subtests).
func startBrokerGlobal() (addr string, cleanup func()) {
	tmpDir, err := os.MkdirTemp("", "fluxmq-integration-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	port := getFreePort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)

	// Write broker stdout to a temp file; C's stdio is fully buffered when piped,
	// so we use a file and poll for the ready line instead of a pipe.
	outFile, err := os.CreateTemp("", "fluxmq-stdout-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp stdout: %v\n", err)
		os.Exit(1)
	}

	cmd := exec.Command(brokerBin,
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--data-dir=%s", tmpDir),
	)
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start broker: %v\n", err)
		os.Exit(1)
	}

	// Wait for the broker to be ready by polling TCP.
	if err := waitForTCP(addr, 10*time.Second); err != nil {
		cmd.Process.Kill()
		fmt.Fprintf(os.Stderr, "broker did not start: %v\n", err)
		os.Exit(1)
	}

	cleanup = func() {
		cmd.Process.Kill()
		cmd.Wait()
		outFile.Close()
		os.Remove(outFile.Name())
		os.RemoveAll(tmpDir)
	}
	return addr, cleanup
}

// startBroker starts a broker for use inside a single test (for isolation).
// extraArgs are appended to the broker command line (e.g. "--session-timeout-ms=3000").
func startBroker(t testing.TB, extraArgs ...string) (addr string, cleanup func()) {
	t.Helper()

	if _, err := os.Stat(brokerBin); os.IsNotExist(err) {
		t.Skip("broker binary not found at " + brokerBin)
	}

	tmpDir := t.TempDir()
	port := getFreePort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)

	outFile, err := os.CreateTemp("", "fluxmq-stdout-*")
	if err != nil {
		t.Fatalf("create temp stdout: %v", err)
	}

	args := []string{
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--data-dir=%s", tmpDir),
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(brokerBin, args...)
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	if err := waitForTCP(addr, 10*time.Second); err != nil {
		cmd.Process.Kill()
		t.Fatalf("broker did not start: %v", err)
	}

	cleanup = func() {
		cmd.Process.Kill()
		cmd.Wait()
		outFile.Close()
		os.Remove(outFile.Name())
	}
	return addr, cleanup
}

// getFreePort binds to :0, reads the assigned port, and closes the listener.
// There is a small TOCTOU window before the broker binds, but in practice this
// is fine for tests on loopback.
func getFreePort() uint16 {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("getFreePort: " + err.Error())
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return uint16(port)
}

// waitForTCP polls addr until a TCP connection succeeds or deadline elapses.
func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", addr)
}

// readPort scans the reader for the startup line and extracts the port number.
// Kept for reference; not used in TCP-poll-based startup.
func readPort(r io.Reader) uint16 {
	re := regexp.MustCompile(`FluxMQ broker listening on port (\d+)`)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if m := re.FindStringSubmatch(line); m != nil {
			n, _ := strconv.ParseUint(m[1], 10, 16)
			return uint16(n)
		}
	}
	return 0
}

// uniqueTopic returns a topic name that is unique within the test run.
func uniqueTopic(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestCreateTopic(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("create-test")
	if err := client.CreateTopic(topic, 3); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	topics, err := client.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}

	found := false
	for _, tm := range topics {
		if tm.Name == topic {
			found = true
			if tm.NumPartitions != 3 {
				t.Errorf("expected 3 partitions, got %d", tm.NumPartitions)
			}
		}
	}
	if !found {
		t.Errorf("topic %q not found in metadata", topic)
	}
}

func TestProduceAndConsume(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("produce-consume")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 10 messages synchronously.
	const N = 10
	for i := 0; i < N; i++ {
		val := fmt.Sprintf("message-%d", i)
		part, off, err := client.Produce(topic, 0, nil, []byte(val))
		if err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
		if part != 0 {
			t.Errorf("expected partition 0, got %d", part)
		}
		if off != uint64(i) {
			t.Errorf("expected offset %d, got %d", i, off)
		}
	}

	// Fetch all 10 messages.
	records, err := client.Fetch(topic, 0, 0, 1024*1024, 1000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(records) != N {
		t.Fatalf("expected %d records, got %d", N, len(records))
	}
	for i, rec := range records {
		expected := fmt.Sprintf("message-%d", i)
		if string(rec.Value) != expected {
			t.Errorf("record[%d]: expected %q, got %q", i, expected, rec.Value)
		}
		if rec.Offset != uint64(i) {
			t.Errorf("record[%d]: expected offset %d, got %d", i, i, rec.Offset)
		}
	}
}

func TestProducerBatching(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("producer-batch")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	producer, err := fluxmq.NewProducer(brokerAddr, fluxmq.ProducerConfig{
		BatchSize:   20,
		LingerMs:    10 * time.Millisecond,
		MaxInFlight: 5,
		Acks:        1,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	const N = 50
	for i := 0; i < N; i++ {
		val := fmt.Sprintf("batch-msg-%d", i)
		if err := producer.Send(topic, nil, []byte(val)); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	if err := producer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Wait a little for all acks.
	time.Sleep(200 * time.Millisecond)

	// Fetch all messages and verify count.
	records, err := client.Fetch(topic, 0, 0, 10*1024*1024, 1000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(records) != N {
		t.Errorf("expected %d records, got %d", N, len(records))
	}

	// Verify all values are present (may be reordered due to concurrency).
	seen := make(map[string]bool)
	for _, rec := range records {
		seen[string(rec.Value)] = true
	}
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("batch-msg-%d", i)
		if !seen[key] {
			t.Errorf("missing message %q", key)
		}
	}
}

func TestConsumerGroup(t *testing.T) {
	// Use a dedicated broker instance so we get a clean state.
	addr, cleanup := startBroker(t)
	defer cleanup()

	client, err := fluxmq.NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("consumer-group")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 10 messages.
	const N = 10
	for i := 0; i < N; i++ {
		val := fmt.Sprintf("cg-msg-%d", i)
		if _, _, err := client.Produce(topic, 0, nil, []byte(val)); err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	// Create a consumer in a group.
	consumer, err := fluxmq.NewConsumer(addr, fluxmq.ConsumerConfig{
		GroupID:   "test-group",
		MaxWaitMs: 500,
		MaxBytes:  1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Subscribe(topic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	received := make(map[string]bool)
	deadline := time.After(15 * time.Second)
	for len(received) < N {
		select {
		case msg := <-consumer.Messages():
			received[string(msg.Value)] = true
		case <-deadline:
			t.Fatalf("timed out waiting for messages; got %d/%d", len(received), N)
		}
	}

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("cg-msg-%d", i)
		if !received[key] {
			t.Errorf("missing message %q", key)
		}
	}
}

func TestOffsetCommitAndFetch(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("offset-test")
	group := "offset-test-group"

	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 5 messages.
	for i := 0; i < 5; i++ {
		if _, _, err := client.Produce(topic, 0, nil, []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	// Commit offset 3.
	if err := client.OffsetCommit(group, topic, 0, 3); err != nil {
		t.Fatalf("OffsetCommit: %v", err)
	}

	// Fetch the committed offset.
	off, err := client.OffsetFetch(group, topic, 0)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if off != 3 {
		t.Errorf("expected committed offset 3, got %d", off)
	}
}

// TestThreeConsumers verifies that three consumers in a group share partitions
// correctly using the RoundRobin strategy and receive all produced messages.
func TestThreeConsumers(t *testing.T) {
	addr, cleanup := startBroker(t)
	defer cleanup()

	client, err := fluxmq.NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("three-consumers")
	const numPartitions = 6
	if err := client.CreateTopic(topic, numPartitions); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 5 messages to each of the 6 partitions = 30 total.
	const msgsPerPartition = 5
	for p := int32(0); p < numPartitions; p++ {
		for i := 0; i < msgsPerPartition; i++ {
			val := fmt.Sprintf("p%d-msg%d", p, i)
			if _, _, err := client.Produce(topic, p, nil, []byte(val)); err != nil {
				t.Fatalf("Produce p%d[%d]: %v", p, i, err)
			}
		}
	}

	// Start 3 consumers with RoundRobin assignment.
	const numConsumers = 3
	cfg := fluxmq.ConsumerConfig{
		GroupID:     uniqueTopic("group"), // unique group so no leftover offsets
		Strategy:    fluxmq.StrategyRoundRobin,
		MaxWaitMs:   500,
		MaxBytes:    1 * 1024 * 1024,
		HeartbeatMs: time.Second,
	}

	consumers := make([]*fluxmq.Consumer, numConsumers)
	for i := range consumers {
		c, err := fluxmq.NewConsumer(addr, cfg)
		if err != nil {
			t.Fatalf("NewConsumer[%d]: %v", i, err)
		}
		if err := c.Subscribe(topic); err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		consumers[i] = c
	}
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()

	// Collect all 30 messages across all consumers.
	received := make(map[string]bool)
	deadline := time.After(30 * time.Second)
	total := numPartitions * msgsPerPartition

	for len(received) < total {
		// Fan-in from all consumer channels.
		for _, c := range consumers {
			select {
			case msg := <-c.Messages():
				received[string(msg.Value)] = true
			default:
			}
		}
		if len(received) < total {
			select {
			case <-deadline:
				t.Fatalf("timed out: got %d/%d messages", len(received), total)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

// TestConsumerGracefulRebalance verifies that when a consumer calls Close()
// (which sends LeaveGroup), the remaining consumer immediately takes over the
// departed consumer's partitions without waiting for a session timeout.
func TestConsumerGracefulRebalance(t *testing.T) {
	addr, cleanup := startBroker(t)
	defer cleanup()

	client, err := fluxmq.NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("graceful-rebalance")
	if err := client.CreateTopic(topic, 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	group := uniqueTopic("gr-group")
	cfg := fluxmq.ConsumerConfig{
		GroupID:     group,
		MaxWaitMs:   300,
		MaxBytes:    1 * 1024 * 1024,
		HeartbeatMs: time.Second,
	}

	// Start 2 consumers.
	c1, _ := fluxmq.NewConsumer(addr, cfg)
	c1.Subscribe(topic)

	c2, _ := fluxmq.NewConsumer(addr, cfg)
	c2.Subscribe(topic)

	// Give both consumers time to join and stabilize.
	time.Sleep(3 * time.Second)

	// Produce to both partitions so c1 holds one and c2 holds the other.
	for i := 0; i < 4; i++ {
		client.Produce(topic, int32(i%2), nil, []byte(fmt.Sprintf("before-%d", i)))
	}

	// Close c1 gracefully (sends LeaveGroup).
	c1.Close()

	// c2 should rebalance and take over both partitions quickly (<5s).
	// Produce to both partitions and verify c2 eventually receives them.
	time.Sleep(2 * time.Second) // wait for rebalance

	const N = 4
	for i := 0; i < N; i++ {
		client.Produce(topic, int32(i%2), nil, []byte(fmt.Sprintf("after-%d", i)))
	}

	received := make(map[string]bool)
	deadline := time.After(15 * time.Second)
	for len(received) < N {
		select {
		case msg := <-c2.Messages():
			if strings.HasPrefix(string(msg.Value), "after-") {
				received[string(msg.Value)] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for post-rebalance messages; got %d/%d", len(received), N)
		}
	}
	c2.Close()
}

// TestConsumerCrashRebalance verifies that when a consumer stops heartbeating
// (simulating a crash), the broker's reaper evicts it after the session timeout
// and the surviving consumer takes over all partitions.
func TestConsumerCrashRebalance(t *testing.T) {
	const sessionTimeoutMs = 3000

	// Start a broker with a short session timeout so the test completes quickly.
	addr, cleanup := startBroker(t, fmt.Sprintf("--session-timeout-ms=%d", sessionTimeoutMs))
	defer cleanup()

	client, err := fluxmq.NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("crash-rebalance")
	if err := client.CreateTopic(topic, 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	group := uniqueTopic("crash-group")

	// Start the real, surviving consumer.
	survivor, err := fluxmq.NewConsumer(addr, fluxmq.ConsumerConfig{
		GroupID:     group,
		MaxWaitMs:   300,
		MaxBytes:    1 * 1024 * 1024,
		HeartbeatMs: 500 * time.Millisecond, // heartbeat faster than session timeout
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	survivor.Subscribe(topic)
	defer survivor.Close()

	// Give the survivor time to join and become stable as the sole member.
	time.Sleep(time.Second)

	// Simulate a crashed consumer: join the group using the raw Client API but
	// never send a heartbeat. The broker will evict it after sessionTimeoutMs.
	zombie, err := fluxmq.NewClient(addr)
	if err != nil {
		t.Fatalf("zombie NewClient: %v", err)
	}
	// Join without heartbeating. The survivor sees kRebalanceInProgress and
	// re-joins. While waiting for "zombie" to rejoin, the reaper evicts it.
	zombie.JoinGroup(group, topic, "zombie-member-that-never-heartbeats")
	// Close the TCP connection without a LeaveGroup — broker doesn't know it's gone.
	zombie.Close()

	// Wait for: session timeout (3s) + rebalance latency (~2s) + buffer.
	waitDur := time.Duration(sessionTimeoutMs)*time.Millisecond + 5*time.Second
	time.Sleep(waitDur)

	// Now produce to both partitions. The survivor must hold both.
	const N = 6
	for i := 0; i < N; i++ {
		if _, _, err := client.Produce(topic, int32(i%2), nil, []byte(fmt.Sprintf("crash-msg-%d", i))); err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	received := make(map[string]bool)
	deadline := time.After(15 * time.Second)
	for len(received) < N {
		select {
		case msg := <-survivor.Messages():
			received[string(msg.Value)] = true
		case <-deadline:
			t.Fatalf("timed out: survivor got %d/%d messages after crash rebalance", len(received), N)
		}
	}
}

// TestReplication starts a 2-broker cluster, produces messages to the leader,
// waits for replication, kills the leader, and verifies the follower (now the
// new leader) has all the data.
func TestReplication(t *testing.T) {
	if _, err := os.Stat(brokerBin); os.IsNotExist(err) {
		t.Skip("broker binary not found")
	}

	clusterDir := t.TempDir()
	dataDir1 := t.TempDir()
	dataDir2 := t.TempDir()

	port1 := getFreePort()
	port2 := getFreePort()
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)

	start := func(brokerID int, port uint16, dataDir string) (*exec.Cmd, func()) {
		outFile, _ := os.CreateTemp("", "fluxmq-repl-*")
		cmd := exec.Command(brokerBin,
			fmt.Sprintf("--port=%d", port),
			fmt.Sprintf("--data-dir=%s", dataDir),
			fmt.Sprintf("--cluster-dir=%s", clusterDir),
			"--broker-host=127.0.0.1",
			fmt.Sprintf("--broker-id=%d", brokerID),
			"--replication-factor=2",
			"--replica-lag-ms=200",
			"--broker-timeout-ms=2000",
		)
		cmd.Stdout = outFile
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start broker %d: %v", brokerID, err)
		}
		return cmd, func() {
			cmd.Process.Kill()
			cmd.Wait()
			outFile.Close()
			os.Remove(outFile.Name())
		}
	}

	cmd1, cleanup1 := start(1, port1, dataDir1)
	defer cleanup1()
	cmd2, cleanup2 := start(2, port2, dataDir2)
	defer cleanup2()

	if err := waitForTCP(addr1, 10*time.Second); err != nil {
		t.Fatalf("broker 1 not ready: %v", err)
	}
	if err := waitForTCP(addr2, 10*time.Second); err != nil {
		t.Fatalf("broker 2 not ready: %v", err)
	}

	// Give brokers time to discover each other via the cluster dir.
	time.Sleep(3 * time.Second)

	// Connect via ClusterClient using broker 1 as seed.
	cc, err := fluxmq.NewClusterClient(addr1)
	if err != nil {
		t.Fatalf("NewClusterClient: %v", err)
	}
	defer cc.Close()

	// Create a topic with replication-factor=2.
	client1, err := fluxmq.NewClient(addr1)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client1.Close()

	topic := uniqueTopic("repl-test")
	if err := client1.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Wait for leader to be elected for partition 0.
	if err := cc.WaitForLeader(topic, 1, 10*time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Produce messages via ClusterClient.
	const N = 10
	for i := 0; i < N; i++ {
		val := fmt.Sprintf("repl-msg-%d", i)
		if _, _, err := cc.Produce(topic, 0, nil, []byte(val)); err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	// Wait for broker 2's MaintenanceLoop to discover the topic (~1s) and
	// for all records to replicate (replica_lag_ms=200ms).
	time.Sleep(3 * time.Second)

	// Kill broker 1 (the leader since it created the topic).
	cmd1.Process.Kill()
	cmd1.Wait()

	// Wait for broker 2 to become the new leader.
	if err := cc.WaitForLeader(topic, 1, 15*time.Second); err != nil {
		t.Fatalf("new leader not elected: %v", err)
	}

	// Fetch from the new leader and verify all messages are present.
	records, err := cc.Fetch(topic, 0, 0, 10*1024*1024, 2000)
	if err != nil {
		t.Fatalf("Fetch after failover: %v", err)
	}
	if len(records) != N {
		t.Fatalf("expected %d records after failover, got %d", N, len(records))
	}
	for i, rec := range records {
		expected := fmt.Sprintf("repl-msg-%d", i)
		if string(rec.Value) != expected {
			t.Errorf("record[%d]: expected %q, got %q", i, expected, rec.Value)
		}
	}

	// Silence cmd2 linter — cleanup2 will kill it.
	_ = cmd2
}

// TestFailover verifies that a ClusterProducer transparently retries to the
// new leader after the original leader broker is killed.
func TestFailover(t *testing.T) {
	if _, err := os.Stat(brokerBin); os.IsNotExist(err) {
		t.Skip("broker binary not found")
	}

	clusterDir := t.TempDir()
	dataDir1 := t.TempDir()
	dataDir2 := t.TempDir()

	port1 := getFreePort()
	port2 := getFreePort()
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)

	startClusterBroker := func(brokerID int, port uint16, dataDir string) (*exec.Cmd, func()) {
		outFile, _ := os.CreateTemp("", "fluxmq-fo-*")
		cmd := exec.Command(brokerBin,
			fmt.Sprintf("--port=%d", port),
			fmt.Sprintf("--data-dir=%s", dataDir),
			fmt.Sprintf("--cluster-dir=%s", clusterDir),
			"--broker-host=127.0.0.1",
			fmt.Sprintf("--broker-id=%d", brokerID),
			"--replication-factor=2",
			"--replica-lag-ms=200",
			"--broker-timeout-ms=2000",
		)
		cmd.Stdout = outFile
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start broker %d: %v", brokerID, err)
		}
		return cmd, func() {
			cmd.Process.Kill()
			cmd.Wait()
			outFile.Close()
			os.Remove(outFile.Name())
		}
	}

	cmd1, cleanup1 := startClusterBroker(1, port1, dataDir1)
	cmd2, cleanup2 := startClusterBroker(2, port2, dataDir2)
	defer cleanup2()

	if err := waitForTCP(addr1, 10*time.Second); err != nil {
		t.Fatalf("broker 1 not ready: %v", err)
	}
	if err := waitForTCP(addr2, 10*time.Second); err != nil {
		t.Fatalf("broker 2 not ready: %v", err)
	}

	// Give brokers time to discover each other.
	time.Sleep(3 * time.Second)

	// Set up topic.
	client1, err := fluxmq.NewClient(addr1)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client1.Close()

	topic := uniqueTopic("failover-test")
	if err := client1.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Connect a ClusterProducer through broker 1.
	producer, err := fluxmq.NewClusterProducer(addr1, fluxmq.ProducerConfig{Acks: 1})
	if err != nil {
		t.Fatalf("NewClusterProducer: %v", err)
	}
	defer producer.Close()

	// Wait for leader.
	cc, err := fluxmq.NewClusterClient(addr1)
	if err != nil {
		t.Fatalf("NewClusterClient: %v", err)
	}
	defer cc.Close()

	if err := cc.WaitForLeader(topic, 1, 10*time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Produce before failover.
	const before = 5
	for i := 0; i < before; i++ {
		if _, _, err := producer.SendSync(topic, 0, nil, []byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatalf("SendSync pre[%d]: %v", i, err)
		}
	}

	// Wait for broker 2's MaintenanceLoop to discover the topic (~1s) and
	// for all records to replicate (replica_lag_ms=200ms).
	time.Sleep(3 * time.Second)

	// Kill broker 1 (original leader).
	cleanup1()
	_ = cmd1

	// Wait for broker 2 to elect itself as the new leader.
	if err := cc.WaitForLeader(topic, 1, 15*time.Second); err != nil {
		t.Fatalf("new leader not elected after failover: %v", err)
	}

	// Produce more messages — ClusterProducer should route to broker 2.
	const after = 5
	for i := 0; i < after; i++ {
		if _, _, err := producer.SendSync(topic, 0, nil, []byte(fmt.Sprintf("post-%d", i))); err != nil {
			t.Fatalf("SendSync post[%d]: %v", i, err)
		}
	}

	// Fetch all and verify.
	records, err := cc.Fetch(topic, 0, 0, 10*1024*1024, 2000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	total := before + after
	if len(records) != total {
		t.Fatalf("expected %d records, got %d", total, len(records))
	}

	_ = cmd2
}

// ─── Idempotent producer tests ────────────────────────────────────────────────

func TestIdempotentProducer(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("idempotent")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Create an idempotent producer.
	p, err := fluxmq.NewProducer(brokerAddr, fluxmq.ProducerConfig{
		Idempotent: true,
		Acks:       1,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	// Produce 10 messages synchronously.
	const N = 10
	for i := 0; i < N; i++ {
		val := fmt.Sprintf("idem-msg-%d", i)
		_, _, err := p.SendSync(topic, 0, nil, []byte(val))
		if err != nil {
			t.Fatalf("SendSync[%d]: %v", i, err)
		}
	}
	p.Close()

	// Fetch all and verify.
	records, err := client.Fetch(topic, 0, 0, 10*1024*1024, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(records) != N {
		t.Fatalf("expected %d records, got %d", N, len(records))
	}
	for i, rec := range records {
		expected := fmt.Sprintf("idem-msg-%d", i)
		if string(rec.Value) != expected {
			t.Errorf("record[%d]: got %q, want %q", i, string(rec.Value), expected)
		}
	}
}

func TestIdempotentDedup(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("idem-dedup")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Allocate a PID and manually send duplicate sequence numbers.
	pid, epoch, err := client.InitProducerId()
	if err != nil {
		t.Fatalf("InitProducerId: %v", err)
	}

	// First produce: seq=0.
	part1, off1, err := client.ProduceIdempotent(topic, 0, nil, []byte("hello"), pid, epoch, 0)
	if err != nil {
		t.Fatalf("ProduceIdempotent[0]: %v", err)
	}
	if part1 != 0 || off1 != 0 {
		t.Fatalf("expected part=0 off=0, got part=%d off=%d", part1, off1)
	}

	// Duplicate: same seq=0.
	part2, off2, err := client.ProduceIdempotent(topic, 0, nil, []byte("hello"), pid, epoch, 0)
	if err != nil {
		t.Fatalf("ProduceIdempotent dup: %v (should succeed with cached offset)", err)
	}
	if off2 != off1 {
		t.Fatalf("duplicate should return same offset %d, got %d", off1, off2)
	}
	_ = part2

	// Verify only one record was written.
	records, err := client.Fetch(topic, 0, 0, 10*1024*1024, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record (dedup), got %d", len(records))
	}
}

func TestIdempotentOutOfOrder(t *testing.T) {
	client, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	topic := uniqueTopic("idem-ooo")
	if err := client.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	pid, epoch, err := client.InitProducerId()
	if err != nil {
		t.Fatalf("InitProducerId: %v", err)
	}

	// Produce seq=0.
	if _, _, err := client.ProduceIdempotent(topic, 0, nil, []byte("msg-0"), pid, epoch, 0); err != nil {
		t.Fatalf("ProduceIdempotent[0]: %v", err)
	}

	// Skip seq=1, try seq=2 → should fail.
	_, _, err = client.ProduceIdempotent(topic, 0, nil, []byte("msg-2"), pid, epoch, 2)
	if err == nil {
		t.Fatal("expected out-of-order error, got nil")
	}
	brokerErr, ok := err.(*fluxmq.BrokerError)
	if !ok {
		t.Fatalf("expected BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != 47 {
		t.Fatalf("expected error code 47 (OutOfOrderSequence), got %d", brokerErr.Code)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// containsString checks whether s contains substr.
func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}

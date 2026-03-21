package fluxmq

// Client is a low-level FluxMQ client that exposes all 9 broker APIs.
type Client struct {
	conn *conn
}

// TopicMeta describes a topic returned by the Metadata API.
type TopicMeta struct {
	Name          string
	NumPartitions int32
}

// Record is a single message returned by the Fetch API.
type Record struct {
	Offset uint64
	Value  []byte
}

// JoinResult is the result of a JoinGroup call.
type JoinResult struct {
	Error    int16
	GenID    int32
	Leader   string
	MemberID string
	Members  []string
}

// Assignment describes partition assignments for a group member (used in SyncGroup).
type Assignment struct {
	MemberID   string
	Partitions []int32
}

// NewClient dials the broker at addr and returns a Client.
func NewClient(addr string) (*Client, error) {
	c, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c}, nil
}

// Close shuts down the underlying TCP connection.
func (c *Client) Close() error {
	c.conn.close()
	return nil
}

// ─── CreateTopic ──────────────────────────────────────────────────────────────
// Request:  [2B topic][4B num_parts]
// Response: [2B error]

func (c *Client) CreateTopic(name string, numPartitions int32) error {
	var enc encoder
	enc.str(name)
	enc.i32(numPartitions)

	resp, err := c.conn.roundtrip(apiCreateTopic, enc.bytes())
	if err != nil {
		return err
	}
	dec := newDecoder(resp)
	return codeToError(dec.i16())
}

// ─── Metadata ─────────────────────────────────────────────────────────────────
// Request:  (empty)
// Response: [4B num_topics][topics: 2B name + 4B num_parts]

func (c *Client) Metadata() ([]TopicMeta, error) {
	resp, err := c.conn.roundtrip(apiMetadata, nil)
	if err != nil {
		return nil, err
	}
	dec := newDecoder(resp)
	n := dec.u32()
	if dec.err != nil {
		return nil, dec.err
	}
	topics := make([]TopicMeta, 0, n)
	for i := uint32(0); i < n; i++ {
		name := dec.str()
		numParts := dec.i32()
		if dec.err != nil {
			return nil, dec.err
		}
		topics = append(topics, TopicMeta{Name: name, NumPartitions: numParts})
	}
	return topics, nil
}

// ─── Produce ──────────────────────────────────────────────────────────────────
// Request:  [2B topic][4B part_id (-1=auto)][2B key_len][key][4B val_len][val]
// Response: [2B error][4B part_id][8B offset]

func (c *Client) Produce(topic string, partID int32, key, value []byte) (actualPart int32, offset uint64, err error) {
	var enc encoder
	enc.str(topic)
	enc.i32(partID)
	enc.bytes16(key)
	enc.bytes32(value)

	resp, err := c.conn.roundtrip(apiProduce, enc.bytes())
	if err != nil {
		return 0, 0, err
	}
	dec := newDecoder(resp)
	code := dec.i16()
	actualPart = dec.i32()
	offset = dec.u64()
	if dec.err != nil {
		return 0, 0, dec.err
	}
	if brokerErr := codeToError(code); brokerErr != nil {
		return actualPart, offset, brokerErr
	}
	return actualPart, offset, nil
}

// ─── Fetch ────────────────────────────────────────────────────────────────────
// Request:  [2B topic][4B part_id][8B fetch_offset][4B max_bytes][4B max_wait_ms]
// Response: [2B error][4B num_records][records: 8B offset + 4B val_len + val]

func (c *Client) Fetch(topic string, partID int32, offset uint64, maxBytes, maxWaitMs uint32) ([]Record, error) {
	var enc encoder
	enc.str(topic)
	enc.i32(partID)
	enc.u64(offset)
	enc.u32(maxBytes)
	enc.u32(maxWaitMs)

	resp, err := c.conn.roundtrip(apiFetch, enc.bytes())
	if err != nil {
		return nil, err
	}
	dec := newDecoder(resp)
	code := dec.i16()
	numRecords := dec.u32()
	if dec.err != nil {
		return nil, dec.err
	}
	if brokerErr := codeToError(code); brokerErr != nil {
		return nil, brokerErr
	}
	records := make([]Record, 0, numRecords)
	for i := uint32(0); i < numRecords; i++ {
		off := dec.u64()
		val := dec.bytes32()
		if dec.err != nil {
			return nil, dec.err
		}
		records = append(records, Record{Offset: off, Value: val})
	}
	return records, nil
}

// ─── JoinGroup ────────────────────────────────────────────────────────────────
// Request:  [2B group][2B topic][2B member]
// Response: [2B error][4B gen_id][2B leader][2B member][4B num_members][members: 2B each]

func (c *Client) JoinGroup(group, topic, memberID string) (JoinResult, error) {
	var enc encoder
	enc.str(group)
	enc.str(topic)
	enc.str(memberID)

	resp, err := c.conn.roundtrip(apiJoinGroup, enc.bytes())
	if err != nil {
		return JoinResult{}, err
	}
	dec := newDecoder(resp)
	code := dec.i16()
	genID := dec.i32()
	leader := dec.str()
	member := dec.str()
	numMembers := dec.u32()
	if dec.err != nil {
		return JoinResult{}, dec.err
	}
	members := make([]string, 0, numMembers)
	for i := uint32(0); i < numMembers; i++ {
		m := dec.str()
		if dec.err != nil {
			return JoinResult{}, dec.err
		}
		members = append(members, m)
	}
	return JoinResult{
		Error:    code,
		GenID:    genID,
		Leader:   leader,
		MemberID: member,
		Members:  members,
	}, nil
}

// ─── SyncGroup ────────────────────────────────────────────────────────────────
// Request:  [2B group][4B gen_id][2B member][4B num_assignments]
//           [assignments: 2B member + 4B num_parts + 4B part_id...]
// Response: [2B error][4B num_parts][4B part_id...]

func (c *Client) SyncGroup(group string, genID int32, memberID string, assignments []Assignment) ([]int32, error) {
	var enc encoder
	enc.str(group)
	enc.i32(genID)
	enc.str(memberID)
	enc.u32(uint32(len(assignments)))
	for _, a := range assignments {
		enc.str(a.MemberID)
		enc.u32(uint32(len(a.Partitions)))
		for _, p := range a.Partitions {
			enc.i32(p)
		}
	}

	resp, err := c.conn.roundtrip(apiSyncGroup, enc.bytes())
	if err != nil {
		return nil, err
	}
	dec := newDecoder(resp)
	code := dec.i16()
	numParts := dec.u32()
	if dec.err != nil {
		return nil, dec.err
	}
	if brokerErr := codeToError(code); brokerErr != nil {
		return nil, brokerErr
	}
	parts := make([]int32, 0, numParts)
	for i := uint32(0); i < numParts; i++ {
		p := dec.i32()
		if dec.err != nil {
			return nil, dec.err
		}
		parts = append(parts, p)
	}
	return parts, nil
}

// ─── Heartbeat ────────────────────────────────────────────────────────────────
// Request:  [2B group][4B gen_id][2B member]
// Response: [2B error]

func (c *Client) Heartbeat(group string, genID int32, memberID string) error {
	var enc encoder
	enc.str(group)
	enc.i32(genID)
	enc.str(memberID)

	resp, err := c.conn.roundtrip(apiHeartbeat, enc.bytes())
	if err != nil {
		return err
	}
	dec := newDecoder(resp)
	return codeToError(dec.i16())
}

// ─── OffsetCommit ─────────────────────────────────────────────────────────────
// Request:  [2B group][2B topic][4B part_id][8B offset]
// Response: [2B error]

func (c *Client) OffsetCommit(group, topic string, partID int32, offset uint64) error {
	var enc encoder
	enc.str(group)
	enc.str(topic)
	enc.i32(partID)
	enc.u64(offset)

	resp, err := c.conn.roundtrip(apiOffsetCommit, enc.bytes())
	if err != nil {
		return err
	}
	dec := newDecoder(resp)
	return codeToError(dec.i16())
}

// ─── OffsetFetch ──────────────────────────────────────────────────────────────
// Request:  [2B group][2B topic][4B part_id]
// Response: [2B error][8B offset]

func (c *Client) OffsetFetch(group, topic string, partID int32) (uint64, error) {
	var enc encoder
	enc.str(group)
	enc.str(topic)
	enc.i32(partID)

	resp, err := c.conn.roundtrip(apiOffsetFetch, enc.bytes())
	if err != nil {
		return 0, err
	}
	dec := newDecoder(resp)
	code := dec.i16()
	offset := dec.u64()
	if dec.err != nil {
		return 0, dec.err
	}
	if brokerErr := codeToError(code); brokerErr != nil {
		return 0, brokerErr
	}
	return offset, nil
}

// ─── API key constants ────────────────────────────────────────────────────────

const (
	apiProduce      uint16 = 0
	apiFetch        uint16 = 1
	apiCreateTopic  uint16 = 2
	apiMetadata     uint16 = 3
	apiJoinGroup    uint16 = 4
	apiSyncGroup    uint16 = 5
	apiHeartbeat    uint16 = 6
	apiOffsetCommit uint16 = 7
	apiOffsetFetch  uint16 = 8
)

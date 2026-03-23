#!/usr/bin/env bash
# ─── FluxMQ Deployment Script ──────────────────────────────────────────────
#
# Terraform-based AWS deployment for the 3-broker FluxMQ cluster.
#
# Usage:
#   ./scripts/deploy.sh <command> [options]
#
# Commands:
#   init         Initialize Terraform
#   plan         Show deployment plan
#   apply        Deploy infrastructure
#   destroy      Tear down cluster
#   output       Show Terraform outputs
#   ssh <node>   SSH to a broker (1, 2, or 3)
#   logs <node>  View broker logs on a node
#   status       Show cluster status
#
# Options:
#   --auto-approve   Skip confirmation prompts
#   --help           Show this help message
#
# Prerequisites:
#   - AWS credentials configured (aws configure or env vars)
#   - terraform installed
#   - SSH key pair in AWS matching var.ssh_key_name

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

TF_DIR="$REPO_ROOT/terraform"
AUTO_APPROVE=false

# ── Parse arguments ─────────────────────────────────────────────────────────

COMMAND="${1:-}"
shift 2>/dev/null || true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --auto-approve) AUTO_APPROVE=true ;;
        --help|-h)      sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        [1-3])          NODE="$1" ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

# ── Validation ──────────────────────────────────────────────────────────────

if [ -z "$COMMAND" ]; then
    error "No command specified. Run: ./scripts/deploy.sh --help"
    exit 1
fi

if ! command -v terraform &>/dev/null; then
    error "terraform not found. Install from: https://developer.hashicorp.com/terraform/install"
    exit 1
fi

# ── Helpers ─────────────────────────────────────────────────────────────────

get_broker_ip() {
    local idx=$((${1:-1} - 1))
    terraform -chdir="$TF_DIR" output -json broker_public_ips | jq -r ".[$idx]"
}

get_ssh_key() {
    terraform -chdir="$TF_DIR" output -json ssh_commands | jq -r '.[0]' | grep -oP '(?<=-i )[\S]+'
}

# ── Commands ────────────────────────────────────────────────────────────────

case "$COMMAND" in
    init)
        info "Initializing Terraform..."
        terraform -chdir="$TF_DIR" init
        success "Terraform initialized"
        ;;

    plan)
        info "Planning deployment..."
        terraform -chdir="$TF_DIR" plan
        ;;

    apply)
        info "Deploying FluxMQ cluster..."
        approve_flag=""
        $AUTO_APPROVE && approve_flag="-auto-approve"
        terraform -chdir="$TF_DIR" apply $approve_flag
        echo ""
        success "Cluster deployed!"
        terraform -chdir="$TF_DIR" output
        ;;

    destroy)
        warn "This will destroy the entire FluxMQ cluster!"
        approve_flag=""
        $AUTO_APPROVE && approve_flag="-auto-approve"
        terraform -chdir="$TF_DIR" destroy $approve_flag
        success "Cluster destroyed"
        ;;

    output)
        terraform -chdir="$TF_DIR" output
        ;;

    ssh)
        if [ -z "${NODE:-}" ]; then
            error "Specify a node: ./scripts/deploy.sh ssh 1"
            exit 1
        fi
        ip=$(get_broker_ip "$NODE")
        info "SSH to broker $NODE ($ip)..."
        key=$(get_ssh_key)
        exec ssh -i "$key" "ubuntu@$ip"
        ;;

    logs)
        if [ -z "${NODE:-}" ]; then
            error "Specify a node: ./scripts/deploy.sh logs 1"
            exit 1
        fi
        ip=$(get_broker_ip "$NODE")
        info "Fetching logs from broker $NODE ($ip)..."
        key=$(get_ssh_key)
        ssh -i "$key" "ubuntu@$ip" "docker logs fluxmq-broker --tail 100 -f"
        ;;

    status)
        info "Cluster status:"
        echo ""
        ips=$(terraform -chdir="$TF_DIR" output -json broker_public_ips | jq -r '.[]')
        i=1
        for ip in $ips; do
            metrics_url="http://${ip}:9093/metrics"
            printf "  Broker %d (%s): " "$i" "$ip"
            if curl -sf --connect-timeout 3 "$metrics_url" &>/dev/null; then
                echo -e "${GREEN}healthy${NC}"
            else
                echo -e "${RED}unreachable${NC}"
            fi
            i=$((i + 1))
        done
        ;;

    *)
        error "Unknown command: $COMMAND"
        echo "Run: ./scripts/deploy.sh --help"
        exit 1
        ;;
esac

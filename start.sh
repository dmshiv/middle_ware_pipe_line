#!/bin/bash
# ============================================================================
# VISA MIDDLEWARE FRAUD DETECTION PIPELINE — START SCRIPT
# ============================================================================
# Starts the entire 14-service fraud detection pipeline.
# Docker Compose healthchecks + depends_on handle all dependency ordering
# automatically — no fragile sleep-based waits.
#
# ARCHITECTURE (dependency graph):
#   zookeeper ──→ kafka ──→ flink-jobmanager ──→ flink-taskmanager
#                   │  ╰──→ logstash (+ elasticsearch)
#                   │  ╰──→ enrichment (+ hazelcast-1)
#                   ╰─────→ simulator (waits for enrichment)
#   elasticsearch ──→ kibana
#   hazelcast-1 ──→ hazelcast-mc
#   prometheus ──→ grafana
#
# USAGE:
#   chmod +x start.sh
#   ./start.sh              # Start everything
#   ./start.sh --skip-build # Skip Docker image builds (faster restart)
# ============================================================================

set -e

# ---------------------------------------------------------------------------
# ANSI COLOR CODES
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------
print_header() {
    echo ""
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} ${BOLD}$1${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
}
print_success() { echo -e "  ${GREEN}✔${NC} $1"; }
print_warning() { echo -e "  ${YELLOW}⚠${NC} $1"; }
print_error()   { echo -e "  ${RED}✘${NC} $1"; }
print_info()    { echo -e "  ${CYAN}ℹ${NC} $1"; }
print_url()     { echo -e "  ${MAGENTA}→${NC} ${BOLD}$1${NC}: ${CYAN}$2${NC}"; }

# Wait for a Docker container to report healthy (or running if no healthcheck).
# Usage: wait_for_healthy "container_name" max_seconds
wait_for_healthy() {
    local container="$1"
    local max_secs="${2:-120}"
    local elapsed=0

    while [[ $elapsed -lt $max_secs ]]; do
        local health
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$container" 2>/dev/null || echo "not-found")

        case "$health" in
            healthy)
                print_success "$container is healthy"
                return 0 ;;
            no-healthcheck)
                # Container has no healthcheck; just confirm it's running
                local running
                running=$(docker inspect --format='{{.State.Running}}' "$container" 2>/dev/null || echo "false")
                if [[ "$running" == "true" ]]; then
                    print_success "$container is running"
                    return 0
                fi ;;
            not-found)
                ;; # container doesn't exist yet, keep waiting
        esac

        elapsed=$((elapsed + 3))
        printf "  ${YELLOW}⏳${NC} Waiting for %-28s (%d/%ds)\r" "$container" "$elapsed" "$max_secs"
        sleep 3
    done

    echo ""
    print_error "$container did not become healthy within ${max_secs}s"
    print_info "Logs: docker logs $container --tail 30"
    return 1
}

# ---------------------------------------------------------------------------
# DETECT COMPOSE COMMAND
# ---------------------------------------------------------------------------
if docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif docker-compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo -e "${RED}✘ Docker Compose not found!${NC}"
    exit 1
fi

# ---------------------------------------------------------------------------
# PRE-FLIGHT CHECKS
# ---------------------------------------------------------------------------
print_header "PRE-FLIGHT CHECKS"

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed!"
    exit 1
fi
print_success "Docker is installed"

if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running!"
    exit 1
fi
print_success "Docker daemon is running"
print_success "Docker Compose is available ($COMPOSE_CMD)"

# Memory check
AVAILABLE_MEM_GB=$(free -g 2>/dev/null | awk '/^Mem:/ {print $7}' || echo "unknown")
if [[ "$AVAILABLE_MEM_GB" != "unknown" ]]; then
    if [[ "$AVAILABLE_MEM_GB" -lt 4 ]]; then
        print_warning "Low memory: ${AVAILABLE_MEM_GB}GB free. Pipeline needs ~6-8GB."
        print_warning "Consider stopping other services (minikube, etc.)"
    else
        print_success "Available memory: ${AVAILABLE_MEM_GB}GB"
    fi
fi

# ---------------------------------------------------------------------------
# PRE-CLEANUP: Remove any orphaned containers/networks from previous runs
# ---------------------------------------------------------------------------
print_info "Cleaning up any orphaned containers from previous runs..."
ORPHAN_NAMES="zookeeper kafka hazelcast-1 hazelcast-2 hazelcast-mc elasticsearch logstash kibana enrichment-service transaction-simulator prometheus grafana"
for name in $ORPHAN_NAMES; do
    docker rm -f "$name" 2>/dev/null || true
done
# Catch flink containers with dynamic names
docker ps -aq --filter "name=flink" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
# Remove stale project networks
for net in $(docker network ls --format '{{.Name}}' 2>/dev/null | grep -i 'visa\|fraud'); do
    docker network rm "$net" 2>/dev/null || true
done
# Remove stale project volumes to prevent Kafka cluster ID clashes
docker volume ls -q --filter "name=visa" 2>/dev/null | xargs -r docker volume rm 2>/dev/null || true
print_success "Pre-cleanup done"

# Port check — warn only, don't block
for pair in "9092:Kafka" "8081:Flink UI" "9200:Elasticsearch" "5601:Kibana" \
            "8085:Hazelcast MC" "9090:Prometheus" "3000:Grafana"; do
    port="${pair%%:*}"
    name="${pair##*:}"
    if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        print_warning "Port $port ($name) already in use"
    fi
done

# ---------------------------------------------------------------------------
# STEP 1: BUILD DOCKER IMAGES
# ---------------------------------------------------------------------------
if [[ "$1" != "--skip-build" ]]; then
    print_header "STEP 1/5: BUILDING DOCKER IMAGES"
    print_info "Building simulator + enrichment images..."
    $COMPOSE_CMD build --parallel 2>&1 | tail -5
    print_success "Docker images built"
else
    print_header "STEP 1/5: SKIPPING BUILD (--skip-build)"
fi

# ---------------------------------------------------------------------------
# STEP 2: START ALL 14 SERVICES
# ---------------------------------------------------------------------------
# Docker Compose handles the full dependency graph via healthchecks:
#   - Kafka waits for Zookeeper (healthy)
#   - Flink JobManager waits for Kafka (healthy)
#   - Flink TaskManager waits for JobManager + Kafka (healthy)
#   - Enrichment waits for Kafka + Hazelcast (healthy)
#   - Simulator waits for Kafka + Enrichment (healthy)
#   - Logstash waits for Kafka + Elasticsearch (healthy)
#   - Kibana waits for Elasticsearch (healthy)
#   - Grafana waits for Prometheus (healthy)
# ---------------------------------------------------------------------------
print_header "STEP 2/5: STARTING ALL SERVICES (14 containers)"
print_info "Docker Compose will start services in dependency order..."
print_info "This takes 2-3 minutes (Elasticsearch & Kafka are slowest)."
echo ""

$COMPOSE_CMD up -d 2>&1

echo ""
print_success "All containers launched"

# ---------------------------------------------------------------------------
# STEP 3: WAIT FOR CRITICAL SERVICES TO BE HEALTHY
# ---------------------------------------------------------------------------
print_header "STEP 3/5: WAITING FOR SERVICES TO BE HEALTHY"

# Infrastructure tier
wait_for_healthy "zookeeper"        60
wait_for_healthy "kafka"            90
wait_for_healthy "hazelcast-1"      60
wait_for_healthy "hazelcast-2"      60
wait_for_healthy "elasticsearch"    120

# Processing tier (depends on infrastructure)
wait_for_healthy "enrichment-service" 90
wait_for_healthy "logstash"          90

# Flink tier
FLINK_JM=$(docker ps --filter "name=flink-jobmanager" --format "{{.Names}}" | head -1)
FLINK_TM=$(docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" | head -1)
if [[ -n "$FLINK_JM" ]]; then
    wait_for_healthy "$FLINK_JM" 120
fi
if [[ -n "$FLINK_TM" ]]; then
    wait_for_healthy "$FLINK_TM" 120
fi

# UI / monitoring tier
wait_for_healthy "kibana"           120
wait_for_healthy "prometheus"        60
wait_for_healthy "grafana"           60
wait_for_healthy "hazelcast-mc"      60

# Simulator has no healthcheck — just confirm it's running
wait_for_healthy "transaction-simulator" 30

echo ""
print_success "All services are up and healthy!"

# ---------------------------------------------------------------------------
# STEP 4: SUBMIT FLINK FRAUD DETECTION JOB
# ---------------------------------------------------------------------------
print_header "STEP 4/5: SUBMITTING FLINK FRAUD DETECTION JOB"

if [[ -z "$FLINK_JM" ]]; then
    print_error "Flink JobManager container not found"
else
    # Check if a job is already running
    RUNNING_JOBS=$(docker exec "$FLINK_JM" flink list -r 2>&1 | grep -c "RUNNING" || true)
    if [[ "$RUNNING_JOBS" -gt 0 ]]; then
        print_info "Flink job is already running ($RUNNING_JOBS jobs)"
    else
        print_info "Submitting fraud detection pipeline..."
        JOB_OUTPUT=$(docker exec "$FLINK_JM" flink run -d /opt/flink/usrlib/fraud-detection-1.0.jar 2>&1 | grep -v WARNING)
        JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP '[a-f0-9]{32}' | head -1)
        if [[ -n "$JOB_ID" ]]; then
            print_success "Flink job submitted: $JOB_ID"
            # Verify it's actually running
            sleep 5
            JOB_STATUS=$(docker exec "$FLINK_JM" flink list -r 2>&1 | grep "$JOB_ID" || true)
            if echo "$JOB_STATUS" | grep -q "RUNNING"; then
                print_success "Flink job is RUNNING"
            else
                print_warning "Flink job submitted but might be initializing — check Flink UI"
            fi
        else
            print_error "Flink job submission failed:"
            echo "$JOB_OUTPUT"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# STEP 5: FINAL STATUS REPORT
# ---------------------------------------------------------------------------
print_header "STEP 5/5: FINAL STATUS"

echo ""
$COMPOSE_CMD ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || $COMPOSE_CMD ps
echo ""

TOTAL=$($COMPOSE_CMD ps -q 2>/dev/null | wc -l)
print_info "Containers running: $TOTAL / expected: 14"

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║${NC}  ${BOLD}${GREEN}✔ PIPELINE IS RUNNING!${NC}                                      ${GREEN}║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
print_url "Kibana Dashboard     " "http://localhost:5601"
print_url "Flink Web UI         " "http://localhost:8081"
print_url "Hazelcast Mgmt Center" "http://localhost:8085"
print_url "Elasticsearch API    " "http://localhost:9200"
print_url "Kafka Broker         " "localhost:9092"
print_url "Prometheus           " "http://localhost:9090"
print_url "Grafana Dashboard    " "http://localhost:3000  (admin/admin)"
print_url "Logstash Monitoring  " "http://localhost:9600"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║${NC}  ${BOLD}QUICK TEST COMMANDS:${NC}                                       ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# Watch live transactions:${NC}                                 ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  docker exec kafka kafka-console-consumer \\                   ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    --bootstrap-server localhost:9092 \\                        ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    --topic transactions.enriched                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# Check blocked transactions:${NC}                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  docker exec kafka kafka-console-consumer \\                   ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    --bootstrap-server localhost:9092 \\                        ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    --topic transactions.blocked                               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# Elasticsearch doc count:${NC}                                 ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  curl -s 'localhost:9200/_cat/indices?v'                      ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# View logs:${NC}                                               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  docker logs -f enrichment-service                            ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  docker logs -f transaction-simulator                         ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# Stop:${NC}  ./stop.sh                                        ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}# Clean:${NC} ./stop.sh --clean  ${NC}(removes data volumes)       ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BOLD}Pipeline started at:${NC} $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

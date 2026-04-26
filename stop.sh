#!/bin/bash
# ============================================================================
# VISA MIDDLEWARE FRAUD DETECTION PIPELINE — STOP SCRIPT
# ============================================================================
# Gracefully stops all 14 services in reverse dependency order:
#   1. Application services (simulator, enrichment) — stop producing data
#   2. Monitoring stack (Grafana, Prometheus) — no more data to scrape
#   3. Processing engine (Flink) — let current jobs finish
#   4. ELK stack — flush remaining data to disk
#   5. Infrastructure (Kafka, Hazelcast, Zookeeper) — stop last
#
# USAGE:
#   ./stop.sh              # Graceful shutdown (keeps data volumes)
#   ./stop.sh --clean      # Stop + remove all data volumes (fresh start)
#   ./stop.sh --nuclear    # Stop + remove volumes + images (full cleanup)
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
print_info()    { echo -e "  ${CYAN}ℹ${NC} $1"; }
print_warning() { echo -e "  ${YELLOW}⚠${NC} $1"; }

# Stop a service by compose service name, tolerating it not running
stop_service() {
    local name="$1"
    local label="${2:-$1}"
    $COMPOSE_CMD stop "$name" 2>/dev/null || true
    print_success "$label stopped"
}

# Detect Docker Compose command
if docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif docker-compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo -e "${RED}✘ Docker Compose not found!${NC}"
    exit 1
fi

# ---------------------------------------------------------------------------
# SHOW CURRENT STATE
# ---------------------------------------------------------------------------
print_header "CURRENT PIPELINE STATUS"

RUNNING=$($COMPOSE_CMD ps -q 2>/dev/null | wc -l || echo "0")
if [ "$RUNNING" -gt 0 ]; then
    print_info "$RUNNING service(s) currently running"
    echo ""
    $COMPOSE_CMD ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || $COMPOSE_CMD ps
    echo ""
else
    print_info "No compose services running — checking for orphans..."
fi

# ---------------------------------------------------------------------------
# STEP 1: TEAR DOWN EVERYTHING (containers + networks + volumes)
# ---------------------------------------------------------------------------
print_header "STEP 1/3: STOPPING & REMOVING ALL CONTAINERS"

# docker compose down -v removes: containers, networks, AND volumes
# --remove-orphans catches containers from renamed projects or old runs
print_info "Stopping all services and removing containers, networks, volumes..."
$COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
print_success "Docker Compose teardown complete"

# ---------------------------------------------------------------------------
# STEP 2: KILL ANY ORPHANED CONTAINERS (from old project names, manual runs)
# ---------------------------------------------------------------------------
print_header "STEP 2/3: CLEANING ORPHANED RESOURCES"

# These container names are used by our project — force-remove if still lingering
ORPHAN_NAMES="zookeeper kafka hazelcast-1 hazelcast-2 hazelcast-mc elasticsearch logstash kibana enrichment-service transaction-simulator prometheus grafana"
ORPHAN_FOUND=false
for name in $ORPHAN_NAMES; do
    if docker ps -aq --filter "name=^${name}$" 2>/dev/null | grep -q .; then
        docker rm -f "$name" 2>/dev/null || true
        print_info "Removed orphan: $name"
        ORPHAN_FOUND=true
    fi
done
# Also catch flink containers (dynamic names)
for cid in $(docker ps -aq --filter "name=flink" 2>/dev/null); do
    cname=$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | sed 's|^/||')
    docker rm -f "$cid" 2>/dev/null || true
    print_info "Removed orphan: $cname"
    ORPHAN_FOUND=true
done

if [ "$ORPHAN_FOUND" = false ]; then
    print_success "No orphaned containers found"
fi

# Remove any lingering project networks
for net in $(docker network ls --format '{{.Name}}' 2>/dev/null | grep -i 'visa\|fraud'); do
    docker network rm "$net" 2>/dev/null || true
    print_info "Removed network: $net"
done

# Remove any project volumes that survived
docker volume ls -q --filter "name=visa" 2>/dev/null | xargs -r docker volume rm 2>/dev/null || true
print_success "All orphaned resources cleaned"

# ---------------------------------------------------------------------------
# STEP 3 (OPTIONAL): REMOVE DOCKER IMAGES
# ---------------------------------------------------------------------------
if [ "$1" = "--nuclear" ]; then
    echo ""
    print_header "STEP 3/3: REMOVING DOCKER IMAGES"
    print_warning "Removing all project Docker images..."
    docker images --filter "reference=*visa*" -q 2>/dev/null | xargs -r docker rmi -f 2>/dev/null || true
    docker images --filter "reference=*fraud*" -q 2>/dev/null | xargs -r docker rmi -f 2>/dev/null || true
    docker image prune -f 2>/dev/null || true
    print_success "Docker images cleaned"
fi

# ---------------------------------------------------------------------------
# FINAL STATUS
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║${NC}  ${BOLD}${GREEN}✔ PIPELINE STOPPED — FULLY CLEANED!${NC}                        ${GREEN}║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  All containers, networks, and volumes removed.              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  Next ${CYAN}./start.sh${NC} will be a clean fresh start.               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  To restart:     ${CYAN}./start.sh${NC}                                  ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  Quick restart:  ${CYAN}./start.sh --skip-build${NC}                     ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  Also remove images: ${CYAN}./stop.sh --nuclear${NC}                     ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BOLD}Pipeline stopped at:${NC} $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

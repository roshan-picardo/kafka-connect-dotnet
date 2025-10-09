#!/bin/bash

# Kafka Connect Integration Test Runner
# This script runs the complete integration test suite with all Docker components

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.test.yml"
TEST_RESULTS_DIR="$SCRIPT_DIR/TestResults"
LOG_FILE="$TEST_RESULTS_DIR/test-run.log"

# Default values
CLEANUP_ON_EXIT=true
VERBOSE=false
REBUILD=false
SERVICES_ONLY=false
WAIT_TIME=60

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run Kafka Connect integration tests with Docker containers.

OPTIONS:
    -h, --help              Show this help message
    -v, --verbose           Enable verbose output
    -r, --rebuild           Force rebuild of Docker images
    -s, --services-only     Start services only (don't run tests)
    -n, --no-cleanup        Don't cleanup containers on exit
    -w, --wait SECONDS      Wait time for services to be ready (default: 60)
    -t, --test PATTERN      Run specific test pattern
    -c, --config FILE       Use custom docker-compose file

EXAMPLES:
    $0                      # Run all tests with default settings
    $0 -v -r                # Verbose output with image rebuild
    $0 -s                   # Start services only for manual testing
    $0 -t "MySqlPluginTests" # Run only MySQL plugin tests
    $0 --no-cleanup         # Keep containers running after tests

EOF
}

# Function to cleanup containers
cleanup() {
    if [ "$CLEANUP_ON_EXIT" = true ]; then
        print_status "Cleaning up containers..."
        docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
        print_success "Cleanup completed"
    else
        print_warning "Containers left running (use --cleanup to remove them)"
        print_status "To manually cleanup: docker-compose -f $COMPOSE_FILE down -v"
    fi
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        exit 1
    fi
    
    # Check .NET SDK
    if ! command -v dotnet &> /dev/null; then
        print_error ".NET SDK is not installed or not in PATH"
        exit 1
    fi
    
    print_success "All prerequisites satisfied"
}

# Function to prepare test environment
prepare_environment() {
    print_status "Preparing test environment..."
    
    # Create test results directory
    mkdir -p "$TEST_RESULTS_DIR"
    
    # Create log file
    touch "$LOG_FILE"
    
    # Change to project root for Docker context
    cd "$PROJECT_ROOT"
    
    print_success "Environment prepared"
}

# Function to build and start services
start_services() {
    print_status "Starting Docker services..."
    
    local build_args=""
    if [ "$REBUILD" = true ]; then
        build_args="--build --force-recreate"
        print_status "Forcing rebuild of Docker images..."
    fi
    
    # Start infrastructure services first
    print_status "Starting infrastructure services (Kafka, databases)..."
    docker-compose -f "$COMPOSE_FILE" up -d $build_args zookeeper kafka mysql postgres mongodb
    
    # Wait for infrastructure to be ready
    print_status "Waiting for infrastructure services to be ready..."
    sleep 20
    
    # Start Kafka Connect
    print_status "Starting Kafka Connect application..."
    docker-compose -f "$COMPOSE_FILE" up -d kafka-connect
    
    # Wait for Kafka Connect to be ready
    print_status "Waiting for Kafka Connect to be ready (timeout: ${WAIT_TIME}s)..."
    local count=0
    while [ $count -lt $WAIT_TIME ]; do
        if docker-compose -f "$COMPOSE_FILE" exec -T kafka-connect wget --no-verbose --tries=1 --spider http://localhost:8083/health 2>/dev/null; then
            print_success "Kafka Connect is ready!"
            break
        fi
        sleep 2
        count=$((count + 2))
        if [ $((count % 10)) -eq 0 ]; then
            print_status "Still waiting for Kafka Connect... (${count}/${WAIT_TIME}s)"
        fi
    done
    
    if [ $count -ge $WAIT_TIME ]; then
        print_error "Kafka Connect failed to start within ${WAIT_TIME} seconds"
        print_status "Checking Kafka Connect logs..."
        docker-compose -f "$COMPOSE_FILE" logs kafka-connect
        exit 1
    fi
    
    print_success "All services are ready!"
}

# Function to show service status
show_service_status() {
    print_status "Service Status:"
    docker-compose -f "$COMPOSE_FILE" ps
    
    print_status "Health Checks:"
    echo "Kafka Connect API: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/health || echo "FAILED")"
    echo "Kafka: $(docker-compose -f "$COMPOSE_FILE" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo "OK" || echo "FAILED")"
    echo "MySQL: $(docker-compose -f "$COMPOSE_FILE" exec -T mysql mysqladmin ping -h localhost -u testuser -ptestpass >/dev/null 2>&1 && echo "OK" || echo "FAILED")"
    echo "PostgreSQL: $(docker-compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U testuser -d testdb >/dev/null 2>&1 && echo "OK" || echo "FAILED")"
    echo "MongoDB: $(docker-compose -f "$COMPOSE_FILE" exec -T mongodb mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1 && echo "OK" || echo "FAILED")"
}

# Function to run tests
run_tests() {
    print_status "Running integration tests..."
    
    local test_args=""
    if [ -n "$TEST_PATTERN" ]; then
        test_args="--filter $TEST_PATTERN"
        print_status "Running tests matching pattern: $TEST_PATTERN"
    fi
    
    local verbose_args=""
    if [ "$VERBOSE" = true ]; then
        verbose_args="--logger console;verbosity=detailed"
    fi
    
    # Run tests using the test-runner container
    print_status "Executing test suite..."
    if docker-compose -f "$COMPOSE_FILE" run --rm test-runner; then
        print_success "All tests passed!"
        return 0
    else
        print_error "Some tests failed!"
        return 1
    fi
}

# Function to collect test results
collect_results() {
    print_status "Collecting test results..."
    
    # Copy test results from container
    docker-compose -f "$COMPOSE_FILE" run --rm -v "$TEST_RESULTS_DIR:/host-results" test-runner \
        sh -c "cp -r /src/TestResults/* /host-results/ 2>/dev/null || true"
    
    # Show summary
    if [ -f "$TEST_RESULTS_DIR/TestResults.trx" ]; then
        print_success "Test results saved to: $TEST_RESULTS_DIR/TestResults.trx"
    fi
    
    if [ -d "$TEST_RESULTS_DIR" ]; then
        print_status "Test artifacts:"
        ls -la "$TEST_RESULTS_DIR/"
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -r|--rebuild)
            REBUILD=true
            shift
            ;;
        -s|--services-only)
            SERVICES_ONLY=true
            shift
            ;;
        -n|--no-cleanup)
            CLEANUP_ON_EXIT=false
            shift
            ;;
        -w|--wait)
            WAIT_TIME="$2"
            shift 2
            ;;
        -t|--test)
            TEST_PATTERN="$2"
            shift 2
            ;;
        -c|--config)
            COMPOSE_FILE="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_status "Starting Kafka Connect Integration Test Suite"
    print_status "============================================="
    
    # Set up cleanup trap
    trap cleanup EXIT
    
    # Check prerequisites
    check_prerequisites
    
    # Prepare environment
    prepare_environment
    
    # Start services
    start_services
    
    # Show service status
    show_service_status
    
    if [ "$SERVICES_ONLY" = true ]; then
        print_success "Services started successfully!"
        print_status "Kafka Connect API: http://localhost:8083"
        print_status "Use Ctrl+C to stop services"
        
        # Wait for user interrupt
        trap 'print_status "Stopping services..."; exit 0' INT
        while true; do
            sleep 1
        done
    else
        # Run tests
        if run_tests; then
            collect_results
            print_success "Integration test suite completed successfully!"
            exit 0
        else
            collect_results
            print_error "Integration test suite failed!"
            exit 1
        fi
    fi
}

# Run main function
main "$@"
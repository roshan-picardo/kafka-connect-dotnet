#!/bin/bash

# Kafka Connect .NET End-to-End Test Runner
# This script manages the complete test lifecycle including Docker environment setup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose.test.yml"
PROJECT_NAME="kafka-connect-tests-$(date +%s)"
TEST_TIMEOUT="30m"

# CBA Corporate Docker Environment Variables
export ARTIFACTORY_ENTERPRISE_USERNAME="${ARTIFACTORY_ENTERPRISE_USERNAME:-testuser}"
export ARTIFACTORY_ENTERPRISE_APIKEY="${ARTIFACTORY_ENTERPRISE_APIKEY:-testkey}"
export GIT_COMMIT_DESC="${GIT_COMMIT_DESC:-1.0.0-test}"

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

# Function to cleanup Docker resources
cleanup() {
    print_status "Cleaning up Docker resources..."
    
    # Stop and remove containers, networks, and volumes but preserve images
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v --remove-orphans 2>/dev/null || true
    
    # Only remove the custom Kafka Connect image (built from Dockerfile)
    print_status "Removing custom Kafka Connect image..."
    local kafka_connect_image=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^kafka-connect-tests.*kafka-connect" | head -1)
    if [ ! -z "$kafka_connect_image" ]; then
        docker rmi "$kafka_connect_image" 2>/dev/null || true
        print_status "Removed Kafka Connect image: $kafka_connect_image"
    fi
    
    # Clean up dangling images and unused networks/volumes (but preserve base images)
    docker system prune -f --volumes --filter "label!=preserve" 2>/dev/null || true
    
    print_status "Docker cleanup completed (base images preserved)"
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
    
    # Check .NET
    if ! command -v dotnet &> /dev/null; then
        print_error ".NET SDK is not installed or not in PATH"
        exit 1
    fi
    
    # Check available memory
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        AVAILABLE_MEM=$(free -g | awk '/^Mem:/{print $7}')
        if [ "$AVAILABLE_MEM" -lt 4 ]; then
            print_warning "Available memory is less than 4GB. Tests may fail due to insufficient resources."
        fi
    fi
    
    print_success "Prerequisites check passed"
}

# Function to build the solution
build_solution() {
    print_status "Building Kafka Connect solution..."
    
    cd ../../src
    dotnet restore
    dotnet build --configuration Debug --no-restore
    
    if [ $? -eq 0 ]; then
        print_success "Solution built successfully"
    else
        print_error "Failed to build solution"
        exit 1
    fi
    
    cd ../tests/Kafka.Connect.Tests
}

# Function to start Docker environment
start_docker_environment() {
    print_status "Starting Docker test environment..."
    
    # Set project name to avoid conflicts
    export COMPOSE_PROJECT_NAME=$PROJECT_NAME
    
    # Start services
    docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d --build --force-recreate
    
    if [ $? -eq 0 ]; then
        print_success "Docker environment started"
    else
        print_error "Failed to start Docker environment"
        cleanup
        exit 1
    fi
}

# Function to wait for services to be healthy
wait_for_services() {
    print_status "Waiting for services to be healthy..."
    
    local services=("test-broker" "test-schema-registry" "test-mongodb" "test-postgres" "test-sqlserver" "test-mysql" "test-mariadb")
    local max_attempts=60
    local attempt=0
    
    for service in "${services[@]}"; do
        print_status "Waiting for $service to be healthy..."
        attempt=0
        
        while [ $attempt -lt $max_attempts ]; do
            if docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps $service | grep -q "Up"; then
                print_success "$service is healthy"
                break
            fi
            
            attempt=$((attempt + 1))
            sleep 5
            
            if [ $attempt -eq $max_attempts ]; then
                print_error "$service failed to become healthy within timeout"
                print_status "Service logs:"
                docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail=50 $service
                cleanup
                exit 1
            fi
        done
    done
    
    # Wait for Kafka Connect specifically
    print_status "Waiting for Kafka Connect to be ready..."
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f http://localhost:6000/health >/dev/null 2>&1; then
            print_success "Kafka Connect is ready"
            break
        fi
        
        attempt=$((attempt + 1))
        sleep 10
        
        if [ $attempt -eq $max_attempts ]; then
            print_error "Kafka Connect failed to become ready within timeout"
            print_status "Kafka Connect logs:"
            docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail=100 test-kafka-connect
            cleanup
            exit 1
        fi
    done
    
    print_success "All services are healthy and ready"
}

# Function to run tests
run_tests() {
    print_status "Running integration tests..."
    
    local test_filter=""
    local verbosity="normal"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --filter)
                test_filter="--filter $2"
                shift 2
                ;;
            --verbose)
                verbosity="detailed"
                shift
                ;;
            --mongodb)
                test_filter="--filter FullyQualifiedName~MongoDbPluginTests"
                shift
                ;;
            --postgresql)
                test_filter="--filter FullyQualifiedName~PostgreSqlPluginTests"
                shift
                ;;
            --mariadb)
                test_filter="--filter FullyQualifiedName~MariaDbPluginTests"
                shift
                ;;
            --mysql)
                test_filter="--filter FullyQualifiedName~MySqlPluginTests"
                shift
                ;;
            --sqlserver)
                test_filter="--filter FullyQualifiedName~SqlServerPluginTests"
                shift
                ;;
            *)
                print_warning "Unknown option: $1"
                shift
                ;;
        esac
    done
    
    # Run the tests
    dotnet test \
        --configuration Debug \
        --no-build \
        --logger "console;verbosity=$verbosity" \
        --logger "trx;LogFileName=test-results.trx" \
        --results-directory ./TestResults \
        $test_filter \
        -- \
        RunConfiguration.TestSessionTimeout=$TEST_TIMEOUT
    
    local test_exit_code=$?
    
    if [ $test_exit_code -eq 0 ]; then
        print_success "All tests passed!"
    else
        print_error "Some tests failed (exit code: $test_exit_code)"
        
        # Show recent logs from Kafka Connect for debugging
        print_status "Recent Kafka Connect logs:"
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail=100 test-kafka-connect
    fi
    
    return $test_exit_code
}

# Function to generate test report
generate_report() {
    print_status "Generating test report..."
    
    if [ -f "./TestResults/test-results.trx" ]; then
        print_success "Test results saved to: ./TestResults/test-results.trx"
    fi
    
    # Show Docker resource usage
    print_status "Docker resource usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" $(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps -q) 2>/dev/null || true
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --filter FILTER    Run tests matching the specified filter"
    echo "  --verbose          Enable detailed test output"
    echo "  --mongodb          Run only MongoDB plugin tests"
    echo "  --postgresql       Run only PostgreSQL plugin tests"
    echo "  --mariadb          Run only MariaDB plugin tests"
    echo "  --mysql            Run only MySQL plugin tests"
    echo "  --sqlserver        Run only SQL Server plugin tests"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Run all tests"
    echo "  $0 --mongodb --verbose               # Run MongoDB tests with detailed output"
    echo "  $0 --filter \"SinkConnector\"         # Run only sink connector tests"
}

# Main execution
main() {
    # Handle help option
    if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    print_status "Starting Kafka Connect .NET End-to-End Tests"
    print_status "Project: $PROJECT_NAME"
    
    # Set trap for cleanup on exit
    trap cleanup EXIT
    
    # Execute test pipeline
    check_prerequisites
    build_solution
    start_docker_environment
    wait_for_services
    
    # Run tests with all arguments passed through
    run_tests "$@"
    local test_result=$?
    
    generate_report
    
    if [ $test_result -eq 0 ]; then
        print_success "Test execution completed successfully!"
    else
        print_error "Test execution completed with failures!"
    fi
    
    return $test_result
}

# Execute main function with all arguments
main "$@"
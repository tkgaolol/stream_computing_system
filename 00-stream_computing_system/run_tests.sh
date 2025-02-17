#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Available test types
AVAILABLE_TESTS="basic,window,wordcount,parallel,retract,recovery,exactlyonce,all"

# Help function
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -t, --test TYPE    Specify test type to run. Available types:"
    echo "                     - basic     : Run basic operator tests (source, sink, map, keyBy, reduce)"
    echo "                     - window    : Run window operation tests (processing-time and event-time)"
    echo "                     - wordcount : Run word count application tests"
    echo "                     - parallel  : Run parallelism and shuffle tests"
    echo "                     - retract   : Run retract computation tests"
    echo "                     - recovery  : Run automatic failure recovery tests"
    echo "                     - exactlyonce: Run exactly-once semantic tests"
    echo "                     - all       : Run all tests (default)"
    echo "  -h, --help         Show this help message"
    echo
    echo "Example:"
    echo "  $0 -t basic        # Run only basic operator tests"
    echo "  $0 -t window       # Run only window operation tests"
    echo "  $0                 # Run all tests"
}

# Parse command line arguments
TEST_TYPE="all"

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            TEST_TYPE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate test type
if [[ ! ",$AVAILABLE_TESTS," == *",$TEST_TYPE,"* ]]; then
    echo "Error: Invalid test type '$TEST_TYPE'"
    echo "Available test types: $AVAILABLE_TESTS"
    exit 1
fi

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_success() {
    echo -e "${GREEN}[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}[WARNING] $(date '+%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1${NC}"
}

# Create test log directory
LOG_DIR="test_logs"
mkdir -p "$LOG_DIR"
TEST_LOG="$LOG_DIR/test_run_$(date '+%Y%m%d_%H%M%S').log"

# Redirect all output to log file while also showing on console
exec 1> >(tee -a "$TEST_LOG") 2>&1

log_info "Starting stream computing system test suite..."
log_info "Logging to: $TEST_LOG"

# Function to check if Kafka is ready
check_kafka() {
    docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 &>/dev/null
    return $?
}

# Function to wait for Kafka to be ready
wait_for_kafka() {
    log_info "Waiting for Kafka to be ready..."
    for i in {1..60}; do
        if check_kafka; then
            log_success "Kafka is ready!"
            return 0
        fi
        echo "Waiting... ($i/60)"
        sleep 2
    done
    log_error "Kafka did not start within 120 seconds"
    return 1
}

# Function to create test topics
create_test_topics() {
    log_info "Creating test topics..."
    
    # Topic for basic operator tests
    log_info "Creating operator-test-topic with 4 partitions..."
    docker exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 4 \
        --topic operator-test-topic
    
    # Topic for window tests
    log_info "Creating window-test-topic with 2 partitions..."
    docker exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 2 \
        --topic window-test-topic
    
    # Topic for word count tests
    log_info "Creating wordcount-test-topic with 3 partitions..."
    docker exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic wordcount-test-topic
}

# Function to produce test data with logging
produce_operator_test_data() {
    log_info "Producing operator test data..."
    local tmp_file=$(mktemp)
    local count=0
    # Generate test data for map, keyBy, and reduce operations
    for i in {1..50}; do
        echo "key1,value$i" >> "$tmp_file"
        echo "key2,value$i" >> "$tmp_file"
        count=$((count + 2))
    done
    
    log_info "Sending $count messages to operator-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic operator-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced operator test data"
}

# Function to produce window test data with logging
produce_window_test_data() {
    log_info "Producing window test data..."
    local tmp_file=$(mktemp)
    local count=0
    # Generate timestamped test data for window operations
    for i in {1..30}; do
        echo "$(date +%s%N),key1,1" >> "$tmp_file"
        count=$((count + 1))
        sleep 0.1  # Add small delay between messages
    done
    
    log_info "Sending $count messages to window-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic window-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced window test data"
}

# Function to produce word count test data with logging
produce_wordcount_test_data() {
    log_info "Producing word count test data..."
    local tmp_file=$(mktemp)
    # Generate sample text data for word count
    echo "hello world hello" >> "$tmp_file"
    echo "stream computing system" >> "$tmp_file"
    echo "hello stream computing" >> "$tmp_file"
    echo "world of streaming" >> "$tmp_file"
    echo "computing in streams" >> "$tmp_file"
    
    log_info "Sending word count test sentences to wordcount-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic wordcount-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced word count test data"
}

# Function to produce retract test data
produce_retract_test_data() {
    log_info "Producing retract test data..."
    local tmp_file=$(mktemp)
    local count=0
    
    # Generate test data for retract operations
    # Format: word,count to test word frequency distribution
    echo "apple,1" >> "$tmp_file"
    echo "banana,1" >> "$tmp_file"
    echo "apple,1" >> "$tmp_file"
    echo "cherry,1" >> "$tmp_file"
    echo "banana,1" >> "$tmp_file"
    echo "apple,1" >> "$tmp_file"
    
    log_info "Sending retract test data to operator-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic operator-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced retract test data"
}

# Function to produce recovery test data
produce_recovery_test_data() {
    log_info "Producing recovery test data..."
    local tmp_file=$(mktemp)
    local count=0
    
    # Generate test data with sequence numbers for tracking
    for i in {1..100}; do
        echo "seq$i,value$i" >> "$tmp_file"
        count=$((count + 1))
    done
    
    log_info "Sending $count messages to operator-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic operator-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced recovery test data"
}

# Function to produce exactly-once test data
produce_exactlyonce_test_data() {
    log_info "Producing exactly-once test data..."
    local tmp_file=$(mktemp)
    local count=0
    
    # Generate test data with unique identifiers
    for i in {1..50}; do
        echo "id$i,value$i" >> "$tmp_file"
        count=$((count + 1))
    done
    
    log_info "Sending $count messages to operator-test-topic..."
    cat "$tmp_file" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic operator-test-topic
    
    rm "$tmp_file"
    log_success "Successfully produced exactly-once test data"
}

# Function to run specific test class with logging
run_test() {
    local test_class=$1
    log_info "Running test class: $test_class"
    local start_time=$(date +%s)
    
    # Create test-specific log file
    local test_log="$LOG_DIR/${test_class}_$(date '+%Y%m%d_%H%M%S').log"
    log_info "Test output will be logged to: $test_log"
    
    # Run test and capture output
    mvn test -Dtest=$test_class -e > "$test_log" 2>&1
    local result=$?
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $result -eq 0 ]; then
        log_success "$test_class completed successfully in ${duration}s"
    else
        log_error "$test_class failed after ${duration}s"
        log_error "Check $test_log for details"
        # Show the last few lines of the log file for immediate feedback
        echo "Last few lines of test output:"
        tail -n 10 "$test_log"
    fi
    
    return $result
}

# Stop any existing containers
log_info "Cleaning up any existing containers..."
docker compose down -v &>/dev/null

# Start Kafka using Docker Compose
log_info "Starting Kafka using Docker Compose..."
docker compose up -d

# Wait for Kafka to be ready
wait_for_kafka || exit 1

# Create test topics
create_test_topics

# Verify topic creation
log_info "Verifying topic creation..."
for topic in operator-test-topic window-test-topic wordcount-test-topic; do
    if ! docker exec kafka kafka-topics --describe --topic $topic --bootstrap-server localhost:9092 &>/dev/null; then
        log_error "Failed to create topic $topic"
        docker compose down -v
        exit 1
    fi
    log_success "Successfully verified topic: $topic"
done

# Run tests in sequence
log_info "Starting test sequence..."

run_basic_test() {
    log_info "=== Basic Operator Tests ==="
    produce_operator_test_data
    run_test "BasicFeaturesTest#testBasicSourceToSink,testMapTransformation,testKeyByOperation"
    OPERATOR_TEST_RESULT=$?
    return $OPERATOR_TEST_RESULT
}

run_window_test() {
    log_info "=== Window Operation Tests ==="
    produce_window_test_data
    run_test "BasicFeaturesTest#testReduceWithWindow"
    WINDOW_TEST_RESULT=$?
    return $WINDOW_TEST_RESULT
}

run_wordcount_test() {
    log_info "=== Word Count Tests ==="
    produce_wordcount_test_data
    run_test "BasicFeaturesTest#testWordCount"
    WORDCOUNT_TEST_RESULT=$?
    return $WORDCOUNT_TEST_RESULT
}

run_parallel_test() {
    log_info "=== Parallelism Tests ==="
    run_test "BasicFeaturesTest#testOperatorParallelism"
    PARALLEL_TEST_RESULT=$?
    return $PARALLEL_TEST_RESULT
}

run_retract_test() {
    log_info "=== Retract Computation Tests ==="
    produce_retract_test_data
    run_test "BasicFeaturesTest#testRetractComputation"
    RETRACT_TEST_RESULT=$?
    return $RETRACT_TEST_RESULT
}

run_recovery_test() {
    log_info "=== Automatic Recovery Tests ==="
    produce_recovery_test_data
    run_test "BasicFeaturesTest#testAutomaticRecovery,testStateRecovery,testAtLeastOnceGuarantee"
    RECOVERY_TEST_RESULT=$?
    return $RECOVERY_TEST_RESULT
}

run_exactlyonce_test() {
    log_info "=== Exactly-Once Semantic Tests ==="
    produce_exactlyonce_test_data
    run_test "BasicFeaturesTest#testExactlyOnceSemantics,testEndToEndExactlyOnce"
    EXACTLYONCE_TEST_RESULT=$?
    return $EXACTLYONCE_TEST_RESULT
}

# Initialize test results
OPERATOR_TEST_RESULT=0
WINDOW_TEST_RESULT=0
WORDCOUNT_TEST_RESULT=0
PARALLEL_TEST_RESULT=0
RETRACT_TEST_RESULT=0
RECOVERY_TEST_RESULT=0
EXACTLYONCE_TEST_RESULT=0

# Run selected tests based on TEST_TYPE
case $TEST_TYPE in
    "basic")
        run_basic_test
        ;;
    "window")
        run_window_test
        ;;
    "wordcount")
        run_wordcount_test
        ;;
    "parallel")
        run_parallel_test
        ;;
    "retract")
        run_retract_test
        ;;
    "recovery")
        run_recovery_test
        ;;
    "exactlyonce")
        run_exactlyonce_test
        ;;
    "all")
        run_basic_test
        run_window_test
        run_wordcount_test
        run_parallel_test
        run_retract_test
        run_recovery_test
        run_exactlyonce_test
        ;;
esac

# Print test summary
log_info "\n=== Test Summary ==="
echo "Test Results:"

# Only show results for tests that were run
if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "basic" ]]; then
    if [ $OPERATOR_TEST_RESULT -eq 0 ]; then
        log_success "✓ Basic Operator Tests"
    else
        log_error "✗ Basic Operator Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "window" ]]; then
    if [ $WINDOW_TEST_RESULT -eq 0 ]; then
        log_success "✓ Window Operation Tests"
    else
        log_error "✗ Window Operation Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "wordcount" ]]; then
    if [ $WORDCOUNT_TEST_RESULT -eq 0 ]; then
        log_success "✓ Word Count Tests"
    else
        log_error "✗ Word Count Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "parallel" ]]; then
    if [ $PARALLEL_TEST_RESULT -eq 0 ]; then
        log_success "✓ Parallelism Tests"
    else
        log_error "✗ Parallelism Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "retract" ]]; then
    if [ $RETRACT_TEST_RESULT -eq 0 ]; then
        log_success "✓ Retract Computation Tests"
    else
        log_error "✗ Retract Computation Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "recovery" ]]; then
    if [ $RECOVERY_TEST_RESULT -eq 0 ]; then
        log_success "✓ Automatic Recovery Tests"
    else
        log_error "✗ Automatic Recovery Tests"
    fi
fi

if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "exactlyonce" ]]; then
    if [ $EXACTLYONCE_TEST_RESULT -eq 0 ]; then
        log_success "✓ Exactly-Once Semantic Tests"
    else
        log_error "✗ Exactly-Once Semantic Tests"
    fi
fi

# Calculate final result based on tests that were run
FINAL_RESULT=0
if [[ "$TEST_TYPE" == "all" ]]; then
    if [ $OPERATOR_TEST_RESULT -eq 0 ] && \
       [ $WINDOW_TEST_RESULT -eq 0 ] && \
       [ $WORDCOUNT_TEST_RESULT -eq 0 ] && \
       [ $PARALLEL_TEST_RESULT -eq 0 ] && \
       [ $RETRACT_TEST_RESULT -eq 0 ] && \
       [ $RECOVERY_TEST_RESULT -eq 0 ] && \
       [ $EXACTLYONCE_TEST_RESULT -eq 0 ]; then
        log_success "\nAll tests passed successfully!"
    else
        log_error "\nSome tests failed. Check the test logs in $LOG_DIR for details."
        FINAL_RESULT=1
    fi
else
    case $TEST_TYPE in
        "basic")
            FINAL_RESULT=$OPERATOR_TEST_RESULT
            ;;
        "window")
            FINAL_RESULT=$WINDOW_TEST_RESULT
            ;;
        "wordcount")
            FINAL_RESULT=$WORDCOUNT_TEST_RESULT
            ;;
        "parallel")
            FINAL_RESULT=$PARALLEL_TEST_RESULT
            ;;
        "retract")
            FINAL_RESULT=$RETRACT_TEST_RESULT
            ;;
        "recovery")
            FINAL_RESULT=$RECOVERY_TEST_RESULT
            ;;
        "exactlyonce")
            FINAL_RESULT=$EXACTLYONCE_TEST_RESULT
            ;;
    esac
    
    if [ $FINAL_RESULT -eq 0 ]; then
        log_success "\nSelected test passed successfully!"
    else
        log_error "\nSelected test failed. Check the test logs in $LOG_DIR for details."
    fi
fi

# Cleanup
log_info "Cleaning up test resources..."
for topic in operator-test-topic window-test-topic wordcount-test-topic; do
    log_info "Deleting topic: $topic"
    docker exec kafka kafka-topics --delete \
        --bootstrap-server localhost:9092 \
        --topic $topic 2>/dev/null || true
done

# Stop Kafka containers
log_info "Stopping Kafka containers..."
docker compose down -v

log_info "Test suite completed. All logs are available in: $LOG_DIR"
exit $FINAL_RESULT 
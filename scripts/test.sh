#!/bin/bash

# Test script to verify all components work correctly

set -e

echo "======================================"
echo "Twitter Trend Momentum - Test Suite"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
run_test() {
    TESTS_RUN=$((TESTS_RUN + 1))
    local test_name=$1
    local command=$2
    
    echo -n "[$TESTS_RUN] Testing: $test_name... "
    if eval "$command" > /dev/null 2>&1; then
        echo -e "${GREEN}PASSED${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}FAILED${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# System Requirements Tests
echo "=== System Requirements ==="
run_test "Java installed" "java -version"
run_test "Maven installed" "mvn -version"
run_test "Docker installed" "docker --version"
run_test "Docker Compose installed" "docker-compose --version"
echo ""

# Project Structure Tests
echo "=== Project Structure ==="
run_test "pom.xml exists" "[ -f pom.xml ]"
run_test "Source structure correct" "[ -d src/main/java/org/streaming ]"
run_test "Scala source structure" "[ -d src/main/scala/org/streaming ]"
run_test "Resources directory" "[ -d src/main/resources ]"
run_test "Scripts directory" "[ -d scripts ]"
echo ""

# Configuration Tests
echo "=== Configuration Files ==="
run_test "application.properties exists" "[ -f src/main/resources/application.properties ]"
run_test "log4j2.xml exists" "[ -f src/main/resources/log4j2.xml ]"
run_test "docker-compose.yml exists" "[ -f docker-compose.yml ]"
echo ""

# Build Tests
echo "=== Maven Build ==="
echo "[*] Building project (this may take a few minutes)..."
if mvn clean package -DskipTests -q 2>/dev/null; then
    echo -e "${GREEN}Build successful${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
    run_test "JAR artifact created" "[ -f target/twitter-trend-momentum-1.0.0.jar ]"
else
    echo -e "${RED}Build failed${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
TESTS_RUN=$((TESTS_RUN + 1))
echo ""

# Docker Tests
echo "=== Docker Configuration ==="
run_test "docker-compose.yml valid" "docker-compose config > /dev/null"
echo ""

# Script Tests
echo "=== Executable Scripts ==="
run_test "build.sh executable" "[ -x scripts/build.sh ]"
run_test "start-infrastructure.sh executable" "[ -x scripts/start-infrastructure.sh ]"
run_test "run-producer.sh executable" "[ -x scripts/run-producer.sh ]"
run_test "run-processor.sh executable" "[ -x scripts/run-processor.sh ]"
echo ""

# Documentation Tests
echo "=== Documentation ==="
run_test "README exists" "[ -f README.md ]"
run_test "QUICKSTART exists" "[ -f QUICKSTART.md ]"
run_test "ALGORITHM doc exists" "[ -f ALGORITHM.md ]"
echo ""

# Print Summary
echo "======================================"
echo "Test Summary"
echo "======================================"
echo "Total Tests: $TESTS_RUN"
echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
else
    echo -e "Failed: ${GREEN}0${NC}"
fi
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Configure Twitter API credentials: input/oAuth-tokens.txt"
    echo "2. Start infrastructure: bash scripts/start-infrastructure.sh"
    echo "3. Run producer: bash scripts/run-producer.sh"
    echo "4. Run processor: bash scripts/run-processor.sh"
    exit 0
else
    echo -e "${RED}✗ Some tests failed. Please fix the issues above.${NC}"
    exit 1
fi

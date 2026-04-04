#!/bin/bash
#
# RollingRaft build script
#
# Usage:
#   ./build.sh              # Build with default settings (Release)
#   ./build.sh debug        # Build Debug version
#   ./build.sh clean        # Clean build directory
#   ./build.sh test         # Build and run tests
#   ./build.sh install      # Build and install
#

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
BUILD_TYPE="Release"
CMAKE_OPTIONS="-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        debug|Debug|DEBUG)
            BUILD_TYPE="Debug"
            shift
            ;;
        clean|Clean|CLEAN)
            echo "Cleaning build directory..."
            rm -rf "${BUILD_DIR}"
            echo "Done."
            exit 0
            ;;
        test|Test|TEST)
            RUN_TESTS=1
            shift
            ;;
        install|Install|INSTALL)
            DO_INSTALL=1
            shift
            ;;
        help|--help|-h)
            echo "Usage: $0 [debug|clean|test|install]"
            echo ""
            echo "Options:"
            echo "  (none)   Build Release version"
            echo "  debug    Build Debug version"
            echo "  clean    Remove build directory"
            echo "  test     Build and run tests"
            echo "  install  Build and install to system"
            echo "  help     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Building RollingRaft (${BUILD_TYPE})"
echo "=========================================="

# Configure
echo "Configuring..."
cmake -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DBUILD_TESTING=ON \
    ${CMAKE_OPTIONS}

# Build
echo "Building..."
cmake --build "${BUILD_DIR}" -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

# Run tests if requested
if [[ "${RUN_TESTS}" == "1" ]]; then
    echo ""
    echo "Running tests..."
    cd "${BUILD_DIR}"
    ctest --output-on-failure
fi

# Install if requested
if [[ "${DO_INSTALL}" == "1" ]]; then
    echo ""
    echo "Installing..."
    cmake --install "${BUILD_DIR}"
fi

echo ""
echo "=========================================="
echo "Build complete!"
echo "=========================================="
echo ""
echo "Binaries are in: ${BUILD_DIR}"
echo ""
echo "To run unit tests:"
echo "  ./build/tests/unit_tests"
echo ""
echo "To run counter example (3 nodes):"
echo "  Terminal 1: ./build/example/counter/counter_server 1 8001 8002 8003"
echo "  Terminal 2: ./build/example/counter/counter_server 2 8002 8001 8003"
echo "  Terminal 3: ./build/example/counter/counter_server 3 8003 8001 8002"
echo ""

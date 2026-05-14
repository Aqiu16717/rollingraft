# RollingRaft — Professional Build Makefile
# ============================================================
# Usage:
#   make              Build Release (default)
#   make debug        Build Debug
#   make asan         Build with AddressSanitizer
#   make tsan         Build with ThreadSanitizer
#   make ubsan        Build with UndefinedBehaviorSanitizer
#   make werror       Build with -Werror
#   make test         Build and run all tests
#   make unit-test    Build and run unit tests
#   make int-test     Build and run integration tests
#   make benchmark    Build benchmarks
#   make format       Run clang-format on all source files
#   make clean        Remove all build directories
#   make install      Install to system (default: /usr/local)
#   make docker-build Build Docker image
#   make docker-test  Run full Docker test suite
#   make diagnose     Run local 3-node cluster diagnostics
# ============================================================

# Project configuration
PROJECT_NAME := rollingraft
BUILD_ROOT   := build
INSTALL_PREFIX ?= /usr/local

# Detect parallelism
NPROCS := $(shell sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

# Detect ccache
CCACHE := $(shell command -v ccache 2>/dev/null)
ifdef CCACHE
  CMAKE_CCACHE := -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
endif

# Common CMake options
CMAKE_COMMON := -DCMAKE_EXPORT_COMPILE_COMMANDS=ON $(CMAKE_CCACHE)

# Build configurations
BUILD_RELEASE := $(BUILD_ROOT)/release
BUILD_DEBUG   := $(BUILD_ROOT)/debug
BUILD_ASAN    := $(BUILD_ROOT)/asan
BUILD_TSAN    := $(BUILD_ROOT)/tsan
BUILD_UBSAN   := $(BUILD_ROOT)/ubsan
BUILD_WERROR  := $(BUILD_ROOT)/werror

# Colors for output
BLUE  := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED   := \033[31m
RESET := \033[0m

# Helper macros
define log_info
	@echo "$(BLUE)[INFO]$(RESET) $(1)"
endef

define log_success
	@echo "$(GREEN)[OK]$(RESET) $(1)"
endef

define log_warn
	@echo "$(YELLOW)[WARN]$(RESET) $(1)"
endef

# Default target
.PHONY: all
all: release

# ============================================================
# Build targets
# ============================================================

.PHONY: release
release: $(BUILD_RELEASE)/CMakeCache.txt
	$(call log_info,"Building Release with $(NPROCS) jobs...")
	@cmake --build $(BUILD_RELEASE) -j$(NPROCS)
	$(call log_success,"Release build complete: $(BUILD_RELEASE)")

.PHONY: debug
debug: $(BUILD_DEBUG)/CMakeCache.txt
	$(call log_info,"Building Debug with $(NPROCS) jobs...")
	@cmake --build $(BUILD_DEBUG) -j$(NPROCS)
	$(call log_success,"Debug build complete: $(BUILD_DEBUG)")

.PHONY: asan
asan: $(BUILD_ASAN)/CMakeCache.txt
	$(call log_info,"Building ASan with $(NPROCS) jobs...")
	@cmake --build $(BUILD_ASAN) -j$(NPROCS)
	$(call log_success,"ASan build complete: $(BUILD_ASAN)")

.PHONY: tsan
tsan: $(BUILD_TSAN)/CMakeCache.txt
	$(call log_info,"Building TSan with $(NPROCS) jobs...")
	@cmake --build $(BUILD_TSAN) -j$(NPROCS)
	$(call log_success,"TSan build complete: $(BUILD_TSAN)")

.PHONY: ubsan
ubsan: $(BUILD_UBSAN)/CMakeCache.txt
	$(call log_info,"Building UBSan with $(NPROCS) jobs...")
	@cmake --build $(BUILD_UBSAN) -j$(NPROCS)
	$(call log_success,"UBSan build complete: $(BUILD_UBSAN)")

.PHONY: werror
werror: $(BUILD_WERROR)/CMakeCache.txt
	$(call log_info,"Building with -Werror...")
	@cmake --build $(BUILD_WERROR) -j$(NPROCS)
	$(call log_success,"Werror build complete: $(BUILD_WERROR)")

# ============================================================
# CMake configuration targets
# ============================================================

$(BUILD_RELEASE)/CMakeCache.txt:
	@mkdir -p $(BUILD_RELEASE)
	@cmake -B $(BUILD_RELEASE) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=ON \
		-DBUILD_BENCHMARK=ON

$(BUILD_DEBUG)/CMakeCache.txt:
	@mkdir -p $(BUILD_DEBUG)
	@cmake -B $(BUILD_DEBUG) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=Debug \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=ON \
		-DBUILD_BENCHMARK=ON

$(BUILD_ASAN)/CMakeCache.txt:
	@mkdir -p $(BUILD_ASAN)
	@cmake -B $(BUILD_ASAN) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=ON \
		-DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer" \
		-DCMAKE_C_FLAGS="-fsanitize=address -fno-omit-frame-pointer" \
		-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
		-DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=address"

$(BUILD_TSAN)/CMakeCache.txt:
	@mkdir -p $(BUILD_TSAN)
	@cmake -B $(BUILD_TSAN) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=OFF \
		-DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
		-DCMAKE_C_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
		-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
		-DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=thread"

$(BUILD_UBSAN)/CMakeCache.txt:
	@mkdir -p $(BUILD_UBSAN)
	@cmake -B $(BUILD_UBSAN) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=ON \
		-DCMAKE_CXX_FLAGS="-fsanitize=undefined -fno-omit-frame-pointer" \
		-DCMAKE_C_FLAGS="-fsanitize=undefined -fno-omit-frame-pointer" \
		-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=undefined" \
		-DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=undefined"

$(BUILD_WERROR)/CMakeCache.txt:
	@mkdir -p $(BUILD_WERROR)
	@cmake -B $(BUILD_WERROR) $(CMAKE_COMMON) \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_TESTING=ON \
		-DBUILD_EXAMPLES=ON \
		-DCMAKE_CXX_FLAGS="-Werror"

# ============================================================
# Test targets
# ============================================================

.PHONY: test
unit-test: $(BUILD_RELEASE)/CMakeCache.txt
	@cmake --build $(BUILD_RELEASE) -j$(NPROCS) --target unit_tests
	$(call log_info,"Running unit tests...")
	@cd $(BUILD_RELEASE) && ctest -R "unit\\." --output-on-failure

.PHONY: int-test
int-test: $(BUILD_RELEASE)/CMakeCache.txt
	@cmake --build $(BUILD_RELEASE) -j$(NPROCS) --target integration_tests
	$(call log_info,"Running integration tests...")
	@cd $(BUILD_RELEASE) && ./tests/integration_tests

.PHONY: test
test: release
	$(call log_info,"Running all tests...")
	@cd $(BUILD_RELEASE) && ctest --output-on-failure
	$(call log_success,"All tests passed!")

.PHONY: test-asan
test-asan: asan
	$(call log_info,"Running tests under ASan...")
	@cd $(BUILD_ASAN) && ctest --output-on-failure
	$(call log_success,"ASan tests passed!")

.PHONY: test-tsan
test-tsan: tsan
	$(call log_info,"Running tests under TSan...")
	@cd $(BUILD_TSAN) && TSAN_OPTIONS=suppressions=$(PWD)/tsan_suppressions.txt ctest --output-on-failure
	$(call log_success,"TSan tests passed!")

.PHONY: test-ubsan
test-ubsan: ubsan
	$(call log_info,"Running tests under UBSan...")
	@cd $(BUILD_UBSAN) && ctest --output-on-failure
	$(call log_success,"UBSan tests passed!")

# ============================================================
# Benchmark targets
# ============================================================

.PHONY: benchmark benchmark-release benchmark-update-baseline
benchmark: $(BUILD_RELEASE)/CMakeCache.txt
	@cmake --build $(BUILD_RELEASE) -j$(NPROCS) \
		--target benchmark_runner benchmark_client benchmark_latency_curve benchmark_failover
	$(call log_success,"Benchmarks built: $(BUILD_RELEASE)/benchmark/")

benchmark-release: $(BUILD_RELEASE)/CMakeCache.txt
	@cmake --build $(BUILD_RELEASE) -j$(NPROCS) --target benchmark_runner
	@./$(BUILD_RELEASE)/benchmark/benchmark_runner --all --output-dir=benchmark/results

benchmark-update-baseline: benchmark-release
	@cp benchmark/results/*.json benchmark/baselines/main/
	@echo "Baseline updated. Commit benchmark/baselines/ to persist."

# ============================================================
# Code formatting
# ============================================================

.PHONY: format
format:
	$(call log_info,"Running clang-format...")
	@find src tests benchmark example include -type f \
		\( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) \
		-exec clang-format -i --style=file {} +
	$(call log_success,"Formatting complete!")

.PHONY: format-check
format-check:
	$(call log_info,"Checking code formatting...")
	@find src tests benchmark example include -type f \
		\( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) \
		-exec clang-format --dry-run --Werror --style=file {} + || \
		{ echo "$(RED)[ERROR]$(RESET) Code formatting check failed. Run 'make format' to fix."; exit 1; }
	$(call log_success,"Code formatting OK!")

# ============================================================
# Install / clean
# ============================================================

.PHONY: install
install: release
	$(call log_info,"Installing to $(INSTALL_PREFIX)...")
	@cmake --install $(BUILD_RELEASE) --prefix $(INSTALL_PREFIX)
	$(call log_success,"Installed to $(INSTALL_PREFIX)")

.PHONY: clean
clean:
	$(call log_warn,"Removing all build directories...")
	@rm -rf $(BUILD_ROOT)
	$(call log_success,"Clean complete!")

.PHONY: distclean
distclean: clean
	$(call log_warn,"Clearing ccache...")
	@ccache -C >/dev/null 2>&1 || true
	$(call log_success,"Distclean complete! All build artifacts and cache cleared.")

# ============================================================
# Docker targets
# ============================================================

.PHONY: docker-build
docker-build:
	$(call log_info,"Building Docker image...")
	@docker build -t $(PROJECT_NAME):latest .
	$(call log_success,"Docker image built: $(PROJECT_NAME):latest")

.PHONY: docker-test
docker-test:
	$(call log_info,"Running Docker test suite...")
	@./scripts/docker-test.sh full
	$(call log_success,"Docker tests complete!")

.PHONY: docker-up
docker-up:
	$(call log_info,"Starting Docker cluster...")
	@docker-compose up -d
	$(call log_success,"Cluster started!")

.PHONY: docker-down
docker-down:
	$(call log_info,"Stopping Docker cluster...")
	@docker-compose down -v
	$(call log_success,"Cluster stopped!")

# ============================================================
# Diagnose
# ============================================================

.PHONY: diagnose
diagnose: release
	$(call log_info,"Running local cluster diagnostics...")
	@./scripts/diagnose.sh

# ============================================================
# Help
# ============================================================

.PHONY: help
help:
	@echo "RollingRaft Build System"
	@echo "========================"
	@echo ""
	@echo "Build targets:"
	@echo "  make release    Build Release (default)"
	@echo "  make debug      Build Debug"
	@echo "  make asan       Build with AddressSanitizer"
	@echo "  make tsan       Build with ThreadSanitizer"
	@echo "  make ubsan      Build with UndefinedBehaviorSanitizer"
	@echo "  make werror     Build with -Werror"
	@echo ""
	@echo "Test targets:"
	@echo "  make test       Run all tests (Release)"
	@echo "  make unit-test  Run unit tests only"
	@echo "  make int-test   Run integration tests only"
	@echo "  make test-asan  Run tests under ASan"
	@echo "  make test-tsan  Run tests under TSan"
	@echo "  make test-ubsan Run tests under UBSan"
	@echo ""
	@echo "Other targets:"
	@echo "  make benchmark      Build benchmarks"
	@echo "  make format         Run clang-format"
	@echo "  make format-check   Check code formatting"
	@echo "  make install        Install to $(INSTALL_PREFIX)"
	@echo "  make clean          Remove all build directories"
	@echo "  make docker-build   Build Docker image"
	@echo "  make docker-test    Run Docker test suite"
	@echo "  make docker-up      Start Docker cluster"
	@echo "  make docker-down    Stop Docker cluster"
	@echo "  make diagnose       Run local 3-node diagnostics"

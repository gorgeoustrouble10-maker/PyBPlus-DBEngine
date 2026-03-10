# PyBPlus-DBEngine Makefile
# English: run, test, bench, clean
# 中文: 启服、测试、压测、清理
# 日本語: 起動、テスト、ベンチ、クリーン

.PHONY: run test bench clean install lint

# Default
all: test

# Install dependencies
install:
	pip install -e .

# Start server (data dir ./data, port 8765)
run:
	python scripts/run_server.py -d ./data -H 127.0.0.1 -P 8765

# Run 76+ core tests
test:
	pytest tests/ -v

# Run concurrency stress benchmark (requires server running)
bench:
	@echo "Ensure server is running: make run (in another terminal)"
	python scripts/benchmark_concurrency.py -H 127.0.0.1 -P 8765 -w 20 -d 30

# Run local benchmark (B+ tree vs dict, no server)
bench-local:
	python scripts/benchmark.py

# Clean caches and temp files
clean:
	rm -rf .pytest_cache
	rm -rf __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache
	rm -rf *.egg-info
	rm -rf build dist
	rm -rf .coverage htmlcov

# Lint & type check
lint:
	black --check src/ scripts/ tests/
	mypy src/ --strict

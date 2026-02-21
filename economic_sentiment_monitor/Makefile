# Convenience Makefile wrapping CMake + Python tasks
.PHONY: build test run clean
build:
	cmake -S . -B build && cmake --build build
test:
	pytest tests/ -v && ctest --test-dir build
run:
	./build/esm_terminal
clean:
	rm -rf build __pycache__
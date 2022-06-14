.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build

duckdb:
	cd ../.. && DISABLE_SANITIZER=1 make debug

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make
	build/example
	build/example_drivermanager

format:
	clang-format --sort-includes=0 -style=file -i main.cpp

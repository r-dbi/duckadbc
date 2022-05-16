

.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build

duckdb:
	cd ../.. && make

main:
	mkdir -p build
	cd build && cmake .. && make
	build/example

format:
	clang-format --sort-includes=0 -style=file -i main.cpp

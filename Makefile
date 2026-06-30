SHELL := /bin/bash

ROWS ?= 100000
LOOPS ?= 1000
FIELDS ?= field_a,field_b,field_c
BENCH_DIR ?= benchdata
DATASET := $(BENCH_DIR)/dataset.jsonl
QUERIES := $(BENCH_DIR)/queries.jsonl
LOOKUPX_RESULT ?= $(BENCH_DIR)/lookupx-result.json
LUCENE_RESULT ?= $(BENCH_DIR)/lucene-result.json

.PHONY: help test bench run example zip \
	install-macos-tools install-mvn check-java check-mvn \
	benchdata bench-lookupx bench-lucene bench-compare bench-clean

help:
	@echo "LookupX targets"
	@echo "  make test                 Run Go tests"
	@echo "  make bench                Run Go indexing/search benchmarks"
	@echo "  make install-mvn          Install Maven on macOS via Homebrew when missing"
	@echo "  make benchdata ROWS=100000 FIELDS=term,group_id,date_key"
	@echo "  make bench-lookupx        Run LookupX benchmark using shared JSONL data"
	@echo "  make bench-lucene         Run Lucene benchmark using same JSONL data"
	@echo "  make bench-compare        Generate data, run LookupX and Lucene, save JSON results"
	@echo "  make bench-clean          Remove generated benchmark data/results"

# Backward-compatible alias.
install-macos-tools: install-mvn

install-mvn:
	@set -euo pipefail; \
	if command -v mvn >/dev/null 2>&1; then \
		echo "mvn already installed: $$(mvn -version | head -n 1)"; \
		exit 0; \
	fi; \
	if [[ "$$(uname -s)" != "Darwin" ]]; then \
		echo "mvn is not installed. Automatic install is only supported on macOS in this Makefile." >&2; \
		echo "Install Maven manually for your OS, then rerun make bench-lucene." >&2; \
		exit 1; \
	fi; \
	if ! command -v brew >/dev/null 2>&1; then \
		echo "Homebrew is required to install Maven on macOS." >&2; \
		echo "Install Homebrew from https://brew.sh, then rerun: make install-mvn" >&2; \
		exit 1; \
	fi; \
	echo "Installing Maven with Homebrew..."; \
	brew update; \
	brew install maven; \
	mvn -version | head -n 1

check-java:
	@set -euo pipefail; \
	if ! command -v java >/dev/null 2>&1; then \
		echo "Java 17+ is required for the Lucene benchmark." >&2; \
		if [[ "$$(uname -s)" == "Darwin" ]] && command -v brew >/dev/null 2>&1; then \
			echo "Install it with: brew install openjdk@17" >&2; \
		else \
			echo "Install Java 17+ for your OS and rerun." >&2; \
		fi; \
		exit 1; \
	fi; \
	java -version 2>&1 | head -n 1

check-mvn: install-mvn

$(DATASET) $(QUERIES):
	@mkdir -p $(BENCH_DIR)
	go run ./cmd/benchgen -rows $(ROWS) -out $(BENCH_DIR) -fields $(FIELDS)
	@echo "Generated: $(DATASET) and $(QUERIES)"

benchdata: $(DATASET) $(QUERIES)

bench-lookupx: $(DATASET) $(QUERIES)
	go run ./cmd/benchlookupx \
		-data $(DATASET) \
		-queries $(QUERIES) \
		-fields $(FIELDS) \
		-loops $(LOOPS) | tee $(LOOKUPX_RESULT)

bench-lucene: check-java check-mvn $(DATASET) $(QUERIES)
	cd bench/lucene && mvn -q compile exec:java \
		-Dexec.args="--data ../../$(DATASET) --queries ../../$(QUERIES) --fields $(FIELDS) --loops $(LOOPS)" | tee ../../$(LUCENE_RESULT)

bench-compare: benchdata
	$(MAKE) bench-lookupx ROWS=$(ROWS) LOOPS=$(LOOPS) FIELDS=$(FIELDS) BENCH_DIR=$(BENCH_DIR)
	$(MAKE) bench-lucene ROWS=$(ROWS) LOOPS=$(LOOPS) FIELDS=$(FIELDS) BENCH_DIR=$(BENCH_DIR)
	@echo "LookupX result: $(LOOKUPX_RESULT)"
	@echo "Lucene result:  $(LUCENE_RESULT)"

bench-clean:
	rm -rf $(BENCH_DIR)

test:
	go test ./...

bench:
	go test ./lookup -bench=. -benchmem

run:
	go run ./cmd/lookupx

example:
	go run ./examples/basic

zip:
	cd .. && zip -r lookupx.zip lookupx -x 'lookupx/data/*' 'lookupx/benchdata/*'

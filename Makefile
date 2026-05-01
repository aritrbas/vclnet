.PHONY: build clean test unit race vet lint fmt tidy shellcheck eol-check normalize-eol check integration multiworker

EXAMPLES := $(wildcard examples/*)
BINS     := $(foreach d,$(EXAMPLES),bin/$(notdir $(d)))

# All Go source files in the module (excluding the bin/ output dir).
GO_FILES := $(shell find . -type f -name '*.go' -not -path './bin/*')

# All shell scripts in the repo (excluding the bin/ output dir).
SH_FILES := $(shell find . -type f -name '*.sh' -not -path './bin/*')

build: $(BINS)

bin/%: examples/%/main.go
	@mkdir -p bin
	go build -o $@ ./examples/$*

clean:
	rm -f bin/*

unit:
	go test -count=1 . ./internal/vclpoll/

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

# `make lint` — read-only gofmt check. Prints each mis-formatted file
# followed by the required diff, and exits non-zero if anything is off.
# Use `make fmt` to apply the fixes in place.
lint:
	@out=$$(gofmt -l -d $(GO_FILES)); \
	if [ -n "$$out" ]; then \
	    echo "$$out"; \
	    echo ""; \
	    echo "gofmt found formatting issues in the files listed above."; \
	    echo "Run 'make fmt' to fix them."; \
	    exit 1; \
	fi
	@echo "gofmt: all Go files are formatted."

# `make fmt` — apply gofmt -s -w in place.
fmt:
	@gofmt -s -w $(GO_FILES)
	@echo "gofmt: reformatted in place."

# `make tidy` — sync go.mod (and create go.sum only if there are external
# deps). Today the module has zero external imports, so this is a no-op,
# but it prunes stale entries the moment a dependency is added.
tidy:
	go mod download
	go mod tidy

# `make shellcheck` — parse-only syntax check on every .sh file via
# `bash -n`. Fails on the first script with a syntax error.
shellcheck:
	@for f in $(SH_FILES); do \
	    printf "%-45s " $$f; \
	    if bash -n $$f; then \
	        echo OK; \
	    else \
	        echo FAIL; \
	        exit 1; \
	    fi; \
	done

# `make eol-check` — read-only. Fails if any text file in the repo has
# CRLF line endings. Skips .git/ and bin/.
eol-check:
	@bad=$$(find . -type f -not -path './.git/*' -not -path './bin/*' \
	    -exec file {} + 2>/dev/null | grep -i CRLF || true); \
	if [ -n "$$bad" ]; then \
	    echo "$$bad"; \
	    echo ""; \
	    echo "Files above still have CRLF line endings."; \
	    echo "Run 'make normalize-eol' to fix them."; \
	    exit 1; \
	fi
	@echo "eol: all files use LF line endings."

# `make normalize-eol` — strip trailing \r from every text file in place.
normalize-eol:
	@count=0; \
	for f in $$(find . -type f -not -path './.git/*' -not -path './bin/*' \
	    -exec file {} + 2>/dev/null | grep -i CRLF | cut -d: -f1); do \
	    sed -i 's/\r$$//' "$$f"; \
	    echo "normalized: $$f"; \
	    count=$$((count+1)); \
	done; \
	echo "normalize-eol: $$count file(s) converted CRLF -> LF."

check: lint vet shellcheck eol-check race build

# Starts and stops an isolated VPP release build.
integration:
	sudo bash test/run_integration.sh

# Usage: make multiworker VPP_WORKERS=4
VPP_WORKERS ?= 4
multiworker:
	sudo bash test/run_multiworker.sh $(VPP_WORKERS)

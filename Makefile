.PHONY: build clean test unit race vet lint fmt tidy shellcheck eol-check normalize-eol check integration multiworker pc pc-clean pc-verify pc-print

EXAMPLES := $(wildcard examples/*)
BINS     := $(foreach d,$(EXAMPLES),bin/$(notdir $(d)))

# All Go source files in the module (excluding the bin/ output dir).
GO_FILES := $(shell find . -type f -name '*.go' -not -path './bin/*')

# All shell scripts in the repo (excluding the bin/ output dir).
SH_FILES := $(shell find . -type f -name '*.sh' -not -path './bin/*')

# ---------------------------------------------------------------------------
# Portable VPP / libvppcom discovery via pkg-config.
#
# `internal/vclpoll/cgo.go` uses `#cgo pkg-config: vppcom`. VPP does not ship
# a `.pc` file today, so this Makefile renders one on demand from the
# `pkgconfig/vppcom.pc.in` template and points PKG_CONFIG_PATH at the local
# `pkgconfig/` directory.
#
# Override any of these on the command line, e.g.:
#   make build VPP_PREFIX=/opt/vpp
#   make build VPP_INCLUDEDIR=/opt/vpp/include VPP_LIBDIR=/opt/vpp/lib
#
# If you have installed a system-wide `vppcom.pc` (or exported your own
# PKG_CONFIG_PATH) you can skip the template entirely by setting
# VCLNET_SKIP_PC=1.
# ---------------------------------------------------------------------------

VPP_PREFIX      ?=
DEB_HOST_MULTIARCH ?= $(shell dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null)
VPP_INCLUDEDIR  ?= $(if $(VPP_PREFIX),$(VPP_PREFIX)/include,)
VPP_LIBDIR      ?= $(if $(VPP_PREFIX),$(VPP_PREFIX)/lib$(if $(DEB_HOST_MULTIARCH),/$(DEB_HOST_MULTIARCH)),)
VPP_VERSION     ?= 0.0.0

PC_DIR          := $(CURDIR)/pkgconfig
PC_TEMPLATE     := $(PC_DIR)/vppcom.pc.in
PC_FILE         := $(PC_DIR)/vppcom.pc

# When we're rendering our own .pc file, prepend our pkgconfig dir. When the
# caller opts out via VCLNET_SKIP_PC=1, respect whatever PKG_CONFIG_PATH they
# already have (possibly empty, meaning "just use system pc files").
ifeq ($(VCLNET_SKIP_PC),1)
GO_ENV := PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)"
BUILD_DEPS :=
else
GO_ENV := PKG_CONFIG_PATH="$(PC_DIR):$(PKG_CONFIG_PATH)"
BUILD_DEPS := pc
endif

# `make pc` — render pkgconfig/vppcom.pc from the shipped template.
# Requires VPP_INCLUDEDIR and VPP_LIBDIR (usually derived from VPP_PREFIX).
pc: $(PC_FILE)

$(PC_FILE): $(PC_TEMPLATE)
	@if [ -z "$(VPP_INCLUDEDIR)" ] || [ -z "$(VPP_LIBDIR)" ]; then \
	    echo "make pc: VPP_PREFIX (or VPP_INCLUDEDIR + VPP_LIBDIR) is required."; \
	    echo "  example: make pc VPP_PREFIX=/opt/vpp"; \
	    echo "  or:      make pc VPP_INCLUDEDIR=/opt/vpp/include VPP_LIBDIR=/opt/vpp/lib"; \
	    exit 1; \
	fi
	@if [ ! -d "$(VPP_INCLUDEDIR)/vcl" ]; then \
	    echo "make pc: warning — $(VPP_INCLUDEDIR)/vcl does not exist (looking for vcl/vppcom.h)."; \
	fi
	@if [ ! -e "$(VPP_LIBDIR)/libvppcom.so" ] && [ ! -e "$(VPP_LIBDIR)/libvppcom.so.$(VPP_VERSION)" ]; then \
	    echo "make pc: warning — no libvppcom.so found in $(VPP_LIBDIR)."; \
	fi
	@sed -e 's|@VPP_PREFIX@|$(VPP_PREFIX)|g' \
	     -e 's|@VPP_INCLUDEDIR@|$(VPP_INCLUDEDIR)|g' \
	     -e 's|@VPP_LIBDIR@|$(VPP_LIBDIR)|g' \
	     -e 's|@VPP_VERSION@|$(VPP_VERSION)|g' \
	     $(PC_TEMPLATE) > $(PC_FILE)
	@echo "wrote $(PC_FILE):"
	@sed 's/^/    /' $(PC_FILE)

pc-clean:
	rm -f $(PC_FILE)

# `make pc-verify` — run pkg-config against the rendered file (or the caller's
# environment when VCLNET_SKIP_PC=1) to confirm discovery works.
pc-verify:
	@$(GO_ENV) pkg-config --exists vppcom \
	    && echo "pkg-config: vppcom OK (modversion=$$($(GO_ENV) pkg-config --modversion vppcom))" \
	    || (echo "pkg-config: could not find vppcom.pc. Run 'make pc VPP_PREFIX=...' first."; exit 1)

pc-print:
	@echo "GO_ENV=$(GO_ENV)"
	@echo "PC_FILE=$(PC_FILE)"
	@echo "VPP_PREFIX=$(VPP_PREFIX)"
	@echo "VPP_INCLUDEDIR=$(VPP_INCLUDEDIR)"
	@echo "VPP_LIBDIR=$(VPP_LIBDIR)"
	@echo "VPP_VERSION=$(VPP_VERSION)"

build: $(BINS)

bin/%: examples/%/main.go $(BUILD_DEPS)
	@mkdir -p bin
	$(GO_ENV) go build -o $@ ./examples/$*

clean: pc-clean
	rm -f bin/*

unit: $(BUILD_DEPS)
	$(GO_ENV) go test -count=1 . ./internal/vclpoll/

test: $(BUILD_DEPS)
	$(GO_ENV) go test -count=1 ./...

race: $(BUILD_DEPS)
	$(GO_ENV) go test -race -count=1 ./...

vet: $(BUILD_DEPS)
	$(GO_ENV) go vet ./...

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
	sudo -E bash test/run_integration.sh

# Usage: make multiworker VPP_WORKERS=4
VPP_WORKERS ?= 4
multiworker:
	sudo -E bash test/run_multiworker.sh $(VPP_WORKERS)

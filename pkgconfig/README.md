# pkgconfig — portable VPP/libvppcom discovery

`internal/vclpoll/cgo.go` links against `libvppcom` via `pkg-config`. Because
VPP does not currently ship a `.pc` file itself, this directory contains a
template (`vppcom.pc.in`) that renders one from a chosen VPP install prefix.

## Quick path (with the top-level Makefile)

```bash
make pc VPP_PREFIX=/opt/vpp
export PKG_CONFIG_PATH="$PWD/pkgconfig:$PKG_CONFIG_PATH"
go build ./...
```

`make build`, `make unit`, `make test`, `make race`, and `make vet` all auto-run
`make pc` first, so if you set `VPP_PREFIX` in the environment (or pass it on
the command line) you never need to invoke it directly:

```bash
VPP_PREFIX=/opt/vpp make build
```

## Manual rendering

Fill in the four placeholders yourself:

| Placeholder         | Meaning                                          | Example                                       |
| ------------------- | ------------------------------------------------ | --------------------------------------------- |
| `@VPP_PREFIX@`      | Install prefix that contains `include/` and libs | `/opt/vpp`                                    |
| `@VPP_INCLUDEDIR@`  | Directory holding `vcl/vppcom.h`                 | `${prefix}/include`                           |
| `@VPP_LIBDIR@`      | Directory holding `libvppcom.so`                 | `${prefix}/lib/x86_64-linux-gnu`              |
| `@VPP_VERSION@`     | Free-form; shown by `pkg-config --modversion`    | `26.10`                                       |

The generated `vppcom.pc` file is git-ignored — check in the template only.

## Required VPP symbol

The pkg-config file only selects include and library paths. It does not prove
that the chosen `libvppcom.so` has the API required by this vclnet revision.
The current CGo bridge directly references `vls_unregister_vcl_worker`:

```bash
nm -D /path/to/libvppcom.so | grep -w vls_unregister_vcl_worker
```

The adjacent VPP review commit `032b24d04` exports this symbol and is the
currently validated build. A stock library without it will fail at link time.
Until the API is in a supported upstream release, carry and pin an equivalent
VPP patch in build and runtime images.

## System-wide install

Instead of `PKG_CONFIG_PATH`, you can copy the rendered file into a directory
that `pkg-config` already searches, for example
`/usr/local/lib/pkgconfig/vppcom.pc`. Verify with:

```bash
pkg-config --modversion vppcom
pkg-config --cflags --libs vppcom
```

Run the symbol check above against the exact library directory reported by the
second command; compile-time and runtime copies must match.

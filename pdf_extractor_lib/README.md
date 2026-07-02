# pdf_extractor_lib

`pdf_extractor_lib` is a Go module with a small C++/cgo boundary for extracting text from PDF bytes.

The library does not implement a PDF parser itself. It wraps Poppler's C++ API through `poppler-cpp`, and exposes a Go API used by `feature_materializer_service`.

## Where the C++ Lives

All native code for this library lives under `cpp/`.

The C++ implementation is:

```text
cpp/src/bridge/go/cgo_pdf_extractor.cpp
```

That file is the native implementation. It loads PDF bytes with Poppler's C++ API, iterates pages, extracts page text, and returns the result through a C-compatible struct.

The matching C ABI header is:

```text
cpp/src/bridge/go/cgo_pdf_extractor.h
```

That header is intentionally C-compatible because it is the boundary consumed by Go cgo. It declares:

```c
pdf_extraction_result_t *pdf_extract_text(const char *data, int size);
void pdf_free_result(pdf_extraction_result_t *result);
```

The Go wrapper is separate from the C++ source:

```text
pkg/extractor.go
```

`pkg/extractor.go` includes the C header and links the static C++ archive with cgo. The Go wrapper owns the public Go API; the C++ file owns the Poppler call.

## How the C++ Is Built

CMake builds the native C++ implementation into a static archive.

The CMake entry points are:

```text
cpp/CMakeLists.txt
cpp/src/CMakeLists.txt
```

`cpp/CMakeLists.txt` discovers `poppler-cpp` through `pkg-config`:

```cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(POPPLER_CPP REQUIRED poppler-cpp)
```

`cpp/src/CMakeLists.txt` creates the static library target:

```text
go_pdf_extractor_lib
```

The target compiles:

```text
cpp/src/bridge/go/cgo_pdf_extractor.cpp
```

and writes:

```text
cpp/build/bin/libgo_pdf_extractor_lib.a
```

## Native Dependency

The C++ code depends on the system `poppler-cpp` package.

It is discovered with `pkg-config`:

```sh
pkg-config --cflags --libs poppler-cpp
```

On macOS with Homebrew, the `.pc` file is provided by the `poppler` formula, usually under:

```text
$(brew --prefix poppler)/lib/pkgconfig/poppler-cpp.pc
```

The repo references that package in two places:

- `cpp/CMakeLists.txt`
- `pkg/extractor.go`

## Build

Build the native static archive:

```sh
make build_cpp
```

This produces:

```text
cpp/build/bin/libgo_pdf_extractor_lib.a
```

Build the Go module with cgo enabled:

```sh
make build_go
```

For Linux artifacts:

```sh
make build_linux
```

This builds both supported Linux variants:

- `cpp/build/linux/musl/arm64/libgo_pdf_extractor_lib.a`
- `cpp/build/linux/glibc/arm64/libgo_pdf_extractor_lib.a`

## Test

Run the isolated C++ tests:

```sh
make test_cpp
```

Run the Go/cgo tests:

```sh
make test
```

The C++ test calls the C ABI directly. The Go test calls the public Go wrapper and verifies the cgo link path.

## Artifact Upload

Upload prebuilt Linux artifacts:

```sh
make upload_artifacts
```

This uploads both `arm64 musl` and `arm64 glibc` builds to CodeArtifact using `scripts/upload_artifacts.sh`.

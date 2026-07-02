.PHONY: test test_cpp install lint build_cpp build_go build_linux upload_artifacts

ENV ?= local-dev
BUILD_TYPE ?= Debug

test:
	@scripts/test.sh $(ENV)

test_cpp:
	@scripts/test_cpp.sh $(ENV)

install:
	@scripts/install.sh

build_cpp:
	@scripts/build_cpp.sh

build_go:
	@scripts/build_go.sh

build_linux:
	@mkdir -p cpp/build/linux/musl/arm64 cpp/build/linux/glibc/arm64
	@scripts/build_linux.sh arm64 $(BUILD_TYPE) musl
	@scripts/build_linux.sh arm64 $(BUILD_TYPE) glibc

upload_artifacts:
	@scripts/upload_artifacts.sh arm64 musl
	@scripts/upload_artifacts.sh arm64 glibc

lint:
	golangci-lint run

debug_cgo:
	cd pkg && go tool cgo -debug-gcc ./extractor.go

#! /usr/bin/env bash

set -euo pipefail

build_jobs()
{
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi

  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu
    return
  fi

  echo 4
}

require_symbol()
{
  local archive_path="$1"
  local symbol_pattern="$2"
  local archive_symbols

  archive_symbols="$(nm "$archive_path")"
  if ! grep -q "$symbol_pattern" <<<"$archive_symbols"; then
    echo "Missing required symbol: $symbol_pattern"
    exit 1
  fi
}

build_cpp()
{
  local current_dir
  local script_dir
  local project_root
  local build_dir
  local archive_path

  current_dir="$(pwd)"
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  project_root="$(cd "${script_dir}/../.." && pwd)"
  trap 'cd "$current_dir"' RETURN

  export CPP_ROOT="$project_root/pdf_extractor_lib/cpp"

  if ! pkg-config --exists poppler-cpp; then
    echo "poppler-cpp pkg-config metadata not found. Run scripts/install-dev.sh first."
    exit 1
  fi

  build_dir="$CPP_ROOT/build"
  archive_path="$build_dir/bin/libgo_pdf_extractor_lib.a"

  rm -rf "$build_dir"
  mkdir -p "$build_dir/bin"

  cmake -S "$CPP_ROOT" -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPP_ROOT="$CPP_ROOT" \
    -DCMAKE_CXX_COMPILER="${CXX:-g++}" \
    -DCMAKE_C_COMPILER="${CC:-gcc}"

  cmake --build "$build_dir" --target go_pdf_extractor_lib -j"$(build_jobs)"

  if [[ ! -f "$archive_path" ]]; then
    echo "Expected archive not found: $archive_path"
    exit 1
  fi

  require_symbol "$archive_path" "pdf_extract_text"
  require_symbol "$archive_path" "pdf_free_result"
}

build_cpp


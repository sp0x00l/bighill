#! /usr/bin/env bash

set -euo pipefail

build_go()
{
  local current_dir
  local script_dir
  local project_root

  current_dir="$(pwd)"
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  project_root="$(cd "${script_dir}/../.." && pwd)"
  trap 'cd "$current_dir"' RETURN

  export CGO_ENABLED=1

  if [ ! -f "$project_root/pdf_extractor_lib/cpp/build/bin/libgo_pdf_extractor_lib.a" ]; then
    "$project_root/pdf_extractor_lib/scripts/build_cpp.sh"
  fi

  cd "$project_root/pdf_extractor_lib"
  go build ./...
}

build_go

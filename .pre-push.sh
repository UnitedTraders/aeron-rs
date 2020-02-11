#!/bin/sh -e

if [ "${1}" = "origin" ]; then
  cargo fmt -- --check
  cargo clippy --all-targets --all -- -Dwarnings
fi
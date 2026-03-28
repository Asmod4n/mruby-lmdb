#!/bin/bash
#
# run_all.sh — Compile and run all LMDB benchmarks
#
# Usage: bash run_all.sh [mruby_binary_path]
#
# If mruby_binary_path is provided, the mruby benchmark will also run.
# Otherwise only C, Python, and Node benchmarks run.

set -e
cd "$(dirname "$0")"

MRUBY_BIN="${1:-}"

SEP="────────────────────────────────────────────────────────────────"

echo "$SEP"
echo "  Embedded Database Benchmark Suite"
echo "  $(date)"
echo "  $(uname -srm)"
echo "$SEP"
echo

# ── Compile C benchmarks ──────────────────────────────────────────────────
echo "Compiling C LMDB benchmark..."
gcc -O3 -march=native -o bench_c_lmdb bench_c_lmdb.c -llmdb -lpthread

# ── Run C LMDB ────────────────────────────────────────────────────────────
echo "$SEP"
./bench_c_lmdb
echo


# ── Run Python LMDB ───────────────────────────────────────────────────────
echo "$SEP"
python3 bench_python_lmdb.py
echo

echo "$SEP"
node bench_node_lmdb.js
echo


echo "$SEP"
../mruby/bin/mruby bench_mruby_lmdb.rb
echo

echo "$SEP"
echo "  Done."
echo "$SEP"

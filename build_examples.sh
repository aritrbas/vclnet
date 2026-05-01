#!/bin/bash
set -e
cd "$(dirname "$0")"

rm -f bin/*

for d in examples/*/; do
    name=$(basename "$d")
    echo "Building $name..."
    go build -o "bin/$name" "./$d"
done

echo "---"
ls -la bin/

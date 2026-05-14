#!/bin/bash
for i in {1..5}; do
  echo "=== Attempt $i/5 ==="
  doppler setup --project 20206205tech --config dev && doppler run -- uv run python step_crawl_document_total.py
  if [ $? -eq 0 ]; then
    echo "SUCCESS on attempt $i"
    exit 0
  fi
  echo "Failed on attempt $i, retrying..."
  sleep 2
done
echo "FAILED after 5 attempts"
exit 1

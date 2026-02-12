#!/usr/bin/env bash
set -euo pipefail

# Run from the mirror directory (or pass it as $1)
root="${1:-.}"

# Count unique event IDs from mirrored pages like:
# .../events/show/<EVENT_ID>/index.html
find "$root" -type f -path '*/events/show/*/index.html' \
  | sed -E 's#.*/events/show/([0-9]+)/index\.html#\1#' \
  | sort -u \
  | wc -l

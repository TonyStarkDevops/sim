#!/bin/bash

input_dir="inputs"
versions=(11 12 21 25 29)
batch_size=12

if [ ! -d "$input_dir" ]; then
    echo "Error: '$input_dir' directory does not exist."
    exit 1
fi

for version in "${versions[@]}"; do
  for file in "$input_dir"/*; do
    if [ -f "$file" ]; then
      echo "python minisim.py -f "$file" -v "$version" -bs $batch_size"
      python schedular/minisim.py -f "$file" -v "$version" -bs "$batch_size"
    fi
  done
done
wait

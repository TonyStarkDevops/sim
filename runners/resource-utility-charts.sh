#!/bin/bash

input_dir="./output"
resources=('local_ssd' 'cache_with_hdd' 'central_hdd')

if [ ! -d "$input_dir" ]; then
    echo "Error: '$input_dir' directory does not exist."
    exit 1
fi

for d in "$input_dir"/*; do
    filename=$(basename "$d")
    if [[ "$filename" != "skip_stage" ]]; then
      for resource in "${resources[@]}"; do
          python visualization/utilization.py -i "$d" -r "$resource"
      done
    fi
done
wait
#!/bin/bash

input_dir="./output"
versions=('e' 't' 'w' 's' 'f' 'd') # e: Execution Time, t: Turnaround TIme, w: Waiting Time, s: Slowdown Time, f: Fairness, d: Finish Time
#versions=('s' 'f') # e: Execution Time, t: Turnaround TIme, w: Waiting Time, s: Slowdown Time
resources=('a')
#resources=('h' 'c' 's' 'a')

if [ ! -d "$input_dir" ]; then
  echo "Error: '$input_dir' directory does not exist."
  exit 1
fi

for d in "$input_dir"/*; do
  filename=$(basename "$d")
  if [[ "$filename" != "skip_stage" ]]; then
    for version in "${versions[@]}"; do
      for resource in "${resources[@]}"; do
        echo python3 visualization/comparative_static.py -i "$d" -t "$version" -r "$resource"
        python3 visualization/comparative_static.py -i "$d" -t "$version" -r "$resource"
      done
    done
  fi
done
wait

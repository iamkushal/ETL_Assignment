#!/bin/sh

cd /app/cache

mkdir -p /app/output

for file in /app/cache/fasta/*.fasta; do
    base_name=$(basename "$file" .fasta)
    output_file="/app/output/${base_name}.csv"
    if [ -f "$output_file" ]; then
        echo "Output file $output_file already exists. Skipping $file."
        continue
    fi
    echo "Processing ${file%.fasta}"
    pangolin --analysis-mode fast "$file" --outfile "$output_file" -t 8
done

awk 'FNR==1 && NR!=1 { next; } { print; }' /app/output/*.csv > /app/output/combined.csv

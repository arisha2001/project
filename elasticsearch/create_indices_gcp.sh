INDEX_NAMES=(
  "covid"
  "etfs"
  "etfs_week_agg"
  "stocks"
  "stocks_week_agg"
)

for name in "${INDEX_NAMES[@]}"
do
  python3 create_index.py --host localhost --mode gcp --mapping_path "./mappings/$name.json"
done

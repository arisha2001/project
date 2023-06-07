INDEX_NAMES=$@

for name in "${INDEX_NAMES[@]}"
do
  python3 create_index.py --host localhost --mode local --mapping_path "./mappings/$name.json"
done

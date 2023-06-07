#### To run python file with command in Terminal it is necessary to add three additional arguments:

1) localhost
2) mode of the reading file (locally or from GCP) ("local"/"gcp")
3) path to the file ("mappings/covid.json"/"")


#### To use local:
run in terminal:
* bash create_indices_local.sh (if you need to create all indices)
* bash create_indices_local_args.sh index_name (if you need to create index)
#### To use gcp:
run in terminal:
* bash create_indices_gcp.sh (if you need to create all indices)
* bash create_indices_gcp_args.sh index_name (if you need to create index)

#### To remove index from cluster (verification):
curl -u elastic:elastic -X DELETE http://localhost:9200/covid

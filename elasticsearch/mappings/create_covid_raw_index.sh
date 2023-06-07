curl -u elastic:elastic -X PUT "http://localhost:9200/covid?include_type_name=true" -H 'Content-Type: application/json' -d'
{
  "settings" : {
      "number_of_shards" : 1,
      "number_of_replicas": 0
  },
  "mappings": {
    "_doc" : {
      "properties": {
        "submission_date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "state": {
          "type": "keyword"
        },
        "tot_cases": {
          "type": "integer"
        },
        "new_case": {
          "type": "integer"
        },
        "tot_death": {
          "type": "integer"
        },
        "new_death": {
          "type": "integer"
        },
        "created_at": {
          "type": "date",
          "format": "yyyy-MM-dd"
        }
      }
    }
  }
}
'
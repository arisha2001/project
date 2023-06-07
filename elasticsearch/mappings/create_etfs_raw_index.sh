curl -u elastic:elastic -X PUT "http://localhost:9200/etfs?include_type_name=true" -H 'Content-Type: application/json' -d'
{
  "settings" : {
      "number_of_shards" : 1,
      "number_of_replicas": 0
  },
  "mappings": {
    "_doc" : {
      "properties": {
        "Ticker": {
          "type": "keyword"
        },
        "Date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "Adj Close": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "Close": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "High": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "Low": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "Cpen": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "Volume": {
          "type": "scaled_float",
          "scaling_factor": 13
        }
      }
    }
  }
}
'
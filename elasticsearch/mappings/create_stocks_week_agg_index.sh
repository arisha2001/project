curl -u elastic:elastic -X PUT "http://localhost:9200/stocks_week_agg?include_type_name=true" -H 'Content-Type: application/json' -d'
{
  "settings" : {
      "number_of_shards" : 1,
      "number_of_replicas": 0
  },
  "mappings": {
    "_doc" : {
      "properties": {
        "ticker": {
          "type": "keyword"
        },
        "first_week_date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "first_week_value": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "first_week_covid_incidents": {
          "type": "integer"
        },
        "max_week_date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "max_week_value": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "max_week_covid_incidents": {
          "type": "integer"
        },
        "min_week_date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "min_week_value": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "min_week_covid_incidents": {
          "type": "integer"
        },
        "last_week_date": {
          "type": "date",
          "format": "yyyy-MM-dd"
        },
        "last_week_value": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "last_week_covid_incidents": {
          "type": "integer"
        },
        "diff_max_first_price": {
          "type": "scaled_float",
          "scaling_factor": 16
        },
        "diff_min_first_price": {
          "type": "scaled_float",
          "scaling_factor": 16
        }
      }
    }
  }
}
'
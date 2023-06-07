# Spark Job to write Elastic

It's necessary to add .jar file to this directory 

From "spark" directory run the following terminal-command:

```sh
spark-submit --jars ./elasticsearch-spark-30_2.12-8.0.0-rc1.jar metrics_calculation_and_write_to_elastic.py --port=9200  --host=localhost --one_node=true --elastic_username=elastic --elastic_password=elastic
```

To check loaded data: 

```sh
curl -u elastic:elastic -X GET "localhost:9200/stocks_week_agg/_search?pretty" -H 'Content-Type: application/json' -d'
{
    "query": {
        "match_all": {}
    }
}
'
```

# Metrics Calculation

```sh
spark-submit metrics_calculation_and_write_to_elastic.py
```

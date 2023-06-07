#### URL for airflow:
http://localhost:8080/

#### URL for elasticsearch:
http://localhost:9200/

#### URL for kibana:
http://localhost:5601/

#### URL for check elasticsearch status:
http://localhost:9200/_cat/health

* Username for enter: elastic
* Password for enter: elastic

#### Execution process:
1) Run certs.sh to create a certificate (This command should only be run once):
   bash certs.sh 

2) Start cluster:
   docker-compose up

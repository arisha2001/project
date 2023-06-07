gcloud compute instances create bigdata-instance \
                        --project=gd-sar-stpractice --labels=team=bigdata \
                        --zone=us-central1-a --machine-type=e2-standard-2 \
                        --service-account=bd-dataproc-admin@gd-sar-stpractice.iam.gserviceaccount.com

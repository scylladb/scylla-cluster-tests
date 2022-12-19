# using a new GCP project

```
# create a key from the project service account
# upload it to our credential store

# upload public key into place
https://console.cloud.google.com/compute/metadata?authuser=1&project=local-ssd-latency&tab=sshkeys

# then you can use the project
export SCT_GCE_PROJECT=gcp-local-ssd-latency

# run a few times, and handle any service that need to be enable,
# untail working/passing
./sct.py prepare-regions -c gce -r us-east1

# create `gcp-local-ssd-latency_service_accounts.json` with the
# details of the backup service account
# and upload to the credential store

# create the runner image
./sct.py create-runner-image --cloud-provider gce --region us-east1

# create jenkins worker
./sct.py create-runner-instance -c gce -r us-east1 -d 9999 --test-id 1234

# rename worker
gcloud beta compute instances stop sct-runner-1-5-instance-1234  --project=local-ssd-latency
gcloud beta compute instances set-name sct-runner-1-5-instance-1234  --project=local-ssd-latency --new-name=gcp-local-ssd-latency-builder1-us-east1
gcloud beta compute instances start gcp-local-ssd-latency-builder1-us-east1  --project=local-ssd-latency

# set static ip
https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address

# configure it in jenkins under name `gcp-local-ssd-latency-builder1-us-east1`
# with following labels
# gcp-local-ssd-latency-builders-us-east1 gcp-local-ssd-latency-builders sct-local-ssd-latency-us-east1

```

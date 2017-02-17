# NIFI PutGCS Processor

This is an [Apache NIFI](https://nifi.apache.org/) processor that uploads files to Google Cloud Platform (GCP) Cloud Storage (GCS). The operation is quite simple, it just needs to know the filename (not starting with a /) and bucket name and authentication keys if it is running outside a GCP compute instance.

## Installation
* mvn pakage
* cp ./target/*.nar $NIFI_HOME/libs

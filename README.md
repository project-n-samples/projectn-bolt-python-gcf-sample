# GCP Cloud Function in Python for Bolt

Sample Python Cloud Function in GCP that utilizes Cloud Storage Client library.

### Requirements

- Python 3.0 or higher

### Build From Source

* Download the source and change to the directory containing the sample code:

```bash
git clone https://gitlab.com/projectn-oss/projectn-bolt-python-gcf-sample.git 

cd projectn-bolt-python-gcf-sample
```

### Deploy

To deploy the function, run the following command:

```bash
gcloud functions deploy <function-name> \
--entry-point bolt_gs_ops_handler \
--runtime python38 \
--trigger-http \
--service-account <service-account-email> \
--project <project-id> \
--region <region> \
--set-env-vars BOLT_URL=<Bolt-Service-Url>
```

### Usage

* The sample Python Cloud Function can be tested from the Google Cloud Console by specifying a 
  triggering event in JSON format.

#### bolt_gs_ops_handler

* bolt_gs_ops_handler represents a Google Cloud Function that is invoked by an HTTP Request.


* bolt_gs_ops_handler accepts the following input parameters as part of the HTTP Request:
    * sdkType - Endpoint to which request is sent. The following values are supported:
        * GS - The Request is sent to Google Cloud Storage.
        * Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable

    * requestType - type of request / operation to be performed. The following requests are supported:
        * list_objects - list objects
        * list_buckets - list buckets
        * get_object_md - head object
        * get_bucket_md - head bucket
        * download_object - get object (md5 hash)
        * upload_object - upload object
        * delete_object - delete object

    * bucket - bucket name

    * key - key name


* Following are examples of various HTTP requests, that can be used to invoke the function.
    * Listing objects from Bolt bucket:
      ```json
        {"requestType": "list_objects", "sdkType": "BOLT", "bucket": "<bucket>"}
      ```
    * Listing buckets from GS:
      ```json
      {"requestType": "list_buckets", "sdkType": "GS"}
      ```
    * Get Bolt object metadata (GET_OBJECT_MD):
      ```json
      {"requestType": "get_object_md", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```
    * Check if GS bucket exists (GET_BUCKET_MD):
      ```json
      {"requestType": "get_bucket_md","sdkType": "GS", "bucket": "<bucket>"}
      ```  
    * Download object (its MD5 Hash) from Bolt:
      ```json
      {"requestType": "download_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```  
    * Upload object to Bolt:
      ```json
      {"requestType": "upload_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>", "value": "<value>"}
      ```  
    * Delete object from Bolt:
      ```json
      {"requestType": "delete_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```


#### bolt_gs_validate_obj_handler

* bolt_gs_validate_obj_handler represents a Google Cloud Function that is invoked by an HTTP Request for performing
  data validation tests. To use this Function, change the entry point to `bolt_gs_validate_obj_handler`


* bolt_gs_validate_obj_handler accepts the following input parameters as part of the HTTP Request:
    * bucket - bucket name

    * key - key name

* Following is an example of a HTTP Request that can be used to invoke the function.
    * Retrieve object(its MD5 hash) from Bolt and GS:

      If the object is gzip encoded, object is decompressed before computing its MD5.
      ```json
      {"bucket": "<bucket>", "key": "<key>"}
      ```

      
#### bolt_gs_perf_handler

* bolt_gs_perf_handler represents a Google Cloud Function that is invoked by an HTTP Request for performing
  Bolt / GS Performance testing. Before running this function, ensure that a source bucket has been crunched by
  `Bolt` with cleaner turned `OFF`. To use this Function, change the entry point to `bolt_gs_perf_handler`.
  

* bolt_gs_perf_handler accepts the following input parameters as part of the HTTP Request:
  * requestType - type of request / operation to be performed. The following requests are supported:
    * list_objects - list objects
    * download_object - download object
    * download_object_ttfb - download object (first byte) 
    * download_object_passthrough - download object (via passthrough) of unmonitored bucket
    * download_object_passthrough_ttfb - download object (first byte via passthrough) of unmonitored bucket 
    * upload_object - upload object
    * delete_object - delete object
    * all - upload, download, delete, list objects (default request if none specified)
      
  * bucket - bucket name
    

* Following are examples of various HTTP requests, that can be used to invoke the function.
    * Measure List objects performance of Bolt / GS.
      ```json
      {"requestType": "list_objects", "bucket": "<bucket>"}
      ```
    * Measure Download object performance of Bolt / GS.
      ```json
      {"requestType": "download_object", "bucket": "<bucket>"}
      ```
    * Measure Download object (first byte) performance of Bolt / GS.
      ```json
      {"requestType": "download_object_ttfb", "bucket": "<bucket>"} 
      ```
    * Measure Download object passthrough performance of Bolt.
      ```json
      {"requestType": "download_object_passthrough", "bucket": "<unmonitored-bucket>"}
      ```
    * Measure Download object passthrough (first byte) performance of Bolt.
      ```json
      {"requestType": "download_object_passthrough_ttfb", "bucket": "<unmonitored-bucket>"}
      ```
    * Measure Upload object performance of Bolt / GS.
      ```json
      {"requestType": "upload_object", "bucket": "<bucket>"}
      ```
    * Measure Delete object performance of Bolt / GS.
      ```json
      {"requestType": "delete_object", "bucket": "<bucket>"}
      ```
    * Measure Upload, Delete, Download, List objects performance of Bolt / GS.
      ```json
      {"requestType": "all", "bucket": "<bucket>"}
      ```
      

#### bolt_auto_heal_handler

* bolt_auto_heal_handler represents a Google Cloud Function that is invoked by an HTTP Request for performing
  Auto-Heal testing.Before running this function, modify `data-cruncher` to use `coldline` tier-class and 
  set `backupduration` and `recoveryscannerperiod` to `1 minute` to ensure that the auto-healing duration is
  within the function execution timeout interval. Crunch a sample bucket having a single object.
  Then delete the single fragment object from the `n-data` bucket. Now run this function, passing the name of the
  crunched bucket along with the single object as input parameters to the function. To use this Function,
  change the entry point to `bolt_auto_heal_handler`.
  

* BoltAutoHealHandler accepts the following input parameters as part of the event:
  * bucket - bucket name
    
  * key - key name
    

* Following is an example of a HTTP Request that can be used to invoke the function.
    * Measure Auto-Heal time of an object in Bolt.
      ```json
      {"bucket": "<bucket>", "key": "<key>"}
      ```
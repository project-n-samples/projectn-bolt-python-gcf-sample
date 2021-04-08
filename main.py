from BoltGSOpsClient import BoltGSOpsClient
from BoltGSPerf import BoltGSPerf
from BoltAutoHeal import BoltAutoHeal


def bolt_gs_ops_handler(request):
    """
    bolt_gs_ops_handler represents a Google Cloud Function that is invoked by an HTTP Request

    bolt_gs_ops_handler accepts the following input parameters as part of the HTTP Request:
    1) sdkType - Endpoint to which request is sent. The following values are supported:
       GS - The Request is sent to Google Cloud Storage.
       Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable

    2) requestType - type of request / operation to be performed. The following requests are supported:
       a) list_objects - list objects
       b) list_buckets - list buckets
       c) get_object_md - get object metadata
       d) get_bucket_md - get bucket metadata
       e) download_object - download object (md5 hash)
       f) upload_object - upload object
       g) delete_object - delete object

    3) bucket - bucket name

    4) key - key name

    Following are examples of various HTTP requests, that can be used to invoke bolt_gs_ops_handler.
    a) Listing objects from Bolt bucket:
        {"requestType": "list_objects_v2", "sdkType": "BOLT", "bucket": "<bucket>"}

    b) Listing buckets from GS:
        {"requestType": "list_buckets", "sdkType": "GS"}

    c) Get Bolt object metadata (GET_OBJECT_MD):
        {"requestType": "get_object_md", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}

    d) Check if GS bucket exists (GET_BUCKET_MD):
        {"requestType": "get_bucket_md","sdkType": "GS", "bucket": "<bucket>"}

    e) Download object (its MD5 Hash) from Bolt:
        {"requestType": "download_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}

    f) Upload object to Bolt:
        {"requestType": "upload_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>", "value": "<value>"}

    g) Delete object from Bolt:
        {"requestType": "delete_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}

    :param request: request object
    :return:response from BoltGSOpsClient
    """
    bolt_gs_ops_client = BoltGSOpsClient()
    return bolt_gs_ops_client.process_event(request)


def bolt_gs_validate_obj_handler(request):
    """
    bolt_gs_validate_obj_handler represents a Google Cloud Function that is invoked by an HTTP Request and
    performs data validation tests.

    bolt_gs_validate_obj_handler accepts the following input parameters as part of the HTTP Request:
    1) bucket - bucket name
    2) key - key name

    Following is an example of a HTTP request that can be used to invoke bolt_gs_validate_obj_handler.
    a) Retrieve object (its MD5 Hash) from Bolt and GS (if the object is gzip encoded then it's decompressed
       before computing MD5):
       {"bucket": "<bucket>", "key": "<key>"}

    :param request: request object
    :return: md5s of object retrieved from Bolt and GS.
    """
    bolt_gs_ops_client = BoltGSOpsClient()
    return bolt_gs_ops_client.validate_obj_md5(request)


def bolt_gs_perf_handler(request):
    """
    bolt_gs_perf_handler represents a Google Cloud Function that is invoked by an HTTP Request for Bolt/GS
    performance testing.

    bolt_gs_perf_handler accepts the following input parameters as part of the HTTP Request:
    1) requestType - type of request / operation to be performed. The following requests are supported:
       a) list_objects - list objects
       b) download_object - download object
       c) download_object_ttfb - download object (first byte)
       d) download_object_passthrough - download object (via passthrough) of unmonitored bucket
       e) download_object_passthrough_ttfb - download object (first byte via passthrough) of unmonitored bucket
       f) upload_object - upload object
       g) delete_object - delete object
       h) all - upload, download, delete, list objects (default request if none specified)

    2) bucket - bucket name

    Following are examples of various HTTP requests that can be used to invoke bolt_gs_perf_handler.
    a) Measure List objects performance of Bolt/GS.
       {"requestType": "list_objects", "bucket": "<bucket>"}

    b) Measure Download object performance of Bolt/GS.
       {"requestType": "download_object", "bucket": "<bucket>"}

    c) Measure Download object (first byte) performance of Bolt/GS.
       {"requestType": "download_object_ttfb", "bucket": "<bucket>"}

    d) Measure Download object passthrough performance of Bolt.
       {"requestType": "download_object_passthrough", "bucket": "<unmonitored-bucket>"}

    e) Measure Download object passthrough (first byte) performance of Bolt.
       {"requestType": "download_object_passthrough_ttfb", "bucket": "<unmonitored-bucket>"}

    f) Measure Upload object performance of Bolt/GS.
       {"requestType": "upload_object", "bucket": "<bucket>"}

    g) Measure Delete object performance of Bolt/GS.
       {"requestType": "delete_object", "bucket": "<bucket>"}

    h) Measure Upload, Delete, Download, List objects performance of Bolt/GS.
       {"requestType": "all", "bucket": "<bucket>"}

    :param request: request Object
    :return: response from BoltGSPerf
    """
    bolt_gs_perf = BoltGSPerf()
    return bolt_gs_perf.process_event(request)


def bolt_auto_heal_handler(request):
    """
    bolt_auto_heal_handler represents a Google Cloud Function that is invoked by an HTTP Request for
    performing auto-heal tests.

    bolt_auto_heal_handler accepts the following input parameters as part of the HTTP Request:
    1) bucket - bucket name
    2) key - key name

    Following is an example of a HTTP request that can be used to invoke bolt_auto_heal_handler.
    a) Measure Auto-Heal time of an object in Bolt.
        {"bucket": "<bucket>", "key": "<key>"}

    :param request: request object
    :return: time taken to auto-heal
    """
    bolt_auto_heal = BoltAutoHeal()
    return bolt_auto_heal.process_event(request)

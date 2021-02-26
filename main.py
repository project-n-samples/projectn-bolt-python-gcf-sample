from BoltGSOpsClient import BoltGSOpsClient


def bolt_gs_ops_handler(request):
    """
    bolt_gs_ops_handler represents a Google Cloud Function that is invoked by an HTTP Request

    bolt_gs_ops_handler accepts the following input parameters as part of the HTTP Request:
    1) sdkType - Endpoint to which request is sent. The following values are supported:
       GS - The Request is sent to Google Cloud Storage.
       Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable

    2) requestType - type of request / operation to be performed. The following requests are supported:
       a) list_objects_v2 - list objects
       b) list_buckets - list buckets
       c) head_object - head object
       d) head_bucket - head bucket
       e) get_object - get object (md5 hash)
       f) put_object - upload object
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

    :param request: request object
    :return: md5s of object retrieved from Bolt and GS.
    """
    bolt_gs_ops_client = BoltGSOpsClient()
    return bolt_gs_ops_client.validate_obj_md5(request)

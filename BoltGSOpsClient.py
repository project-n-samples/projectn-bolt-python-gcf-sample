import json
import os
import gzip
import hashlib
import requests
from google.cloud import storage


class BoltGSOpsClient:
    """
    BoltGSOpsClient processes Http Requests that are received by the functions
    bolt_gs_ops_handler and bolt_gs_validate_obj_handler.
    """

    def __init__(self):
        self._storage_client = None
        self._bolt_url = os.environ.get("BOLT_URL").replace('{region}', self.get_region())

    def process_event(self, request):
        """
        process_event extracts the parameters (sdkType, requestType, bucket/key) from the HTTP Request, uses those
        parameters to send an Object/Bucket CRUD request to Bolt/GS and returns back an appropriate response.

        :param request: request object
        :return: result of the requested operation returned by the endpoint (sdkType)
        """

        # Parse JSON Request.
        request_json = request.get_json()

        if request_json:
            if 'bucket' in request_json:
                bucket_name = request_json['bucket']

            if 'requestType' in request_json:
                request_type = str(request_json['requestType']).upper()

            # request is sent to GS if 'sdkType' is not passed as a parameter in the HTTP Request.
            if 'sdkType' in request_json:
                sdk_type = str(request_json['sdkType']).upper()
            else:
                sdk_type = 'GS'

            if 'key' in request_json:
                object_name = request_json['key']

            if 'value' in request_json:
                value = request_json['value']

        # create an Google/Bolt Storage Client depending on the 'sdkType'.
        if sdk_type == 'GS':
            self._storage_client = storage.Client()
        elif sdk_type == 'BOLT':
            client_options = {"api_endpoint": self._bolt_url}
            self._storage_client = storage.Client(client_options=client_options)

        # Perform a GS / Bolt operation based on the input 'requestType'
        try:
            if request_type == "LIST_OBJECTS":
                return self._list_objects(bucket_name)
            elif request_type == "LIST_BUCKETS":
                return self._list_buckets()
            elif request_type == "GET_BUCKET_MD":
                return self._get_bucket_metadata(bucket_name)
            elif request_type == "GET_OBJECT_MD":
                return self._get_object_metadata(bucket_name, object_name)
            elif request_type == "UPLOAD_OBJECT":
                return self._upload_object(bucket_name, object_name, value)
            elif request_type == "DOWNLOAD_OBJECT":
                return self._download_object(bucket_name,object_name)
            elif request_type == "DELETE_OBJECT":
                return self._delete_object(bucket_name, object_name)
        except Exception as e:
            return {
                'errorMessage': str(e),
                'errorCode': str(1)
            }

    def _list_objects(self, bucket_name):
        """
        Returns a list of objects from the given bucket in Bolt/GS
        :param bucket_name: bucket name
        :return: list of objects
        """
        blobs = self._storage_client.list_blobs(bucket_name)
        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)

        return json.dumps({"objects": blob_names}, indent=4, sort_keys=True)

    def _list_buckets(self):
        """
        Returns list of buckets
        :return: list of buckets.
        """
        buckets = self._storage_client.list_buckets()
        bucket_names = []
        for bucket in buckets:
            bucket_names.append(bucket.name)

        return json.dumps({"buckets": bucket_names}, indent=4, sort_keys=True)

    def _get_bucket_metadata(self, bucket_name):
        """
        Get Bucket Metadata from Bolt/ GS
        :param bucket_name: bucket name
        :return: bucket metadata
        """
        bucket = self._storage_client.get_bucket(bucket_name)

        return {
            'BucketName': bucket.name,
            'Location': bucket.location,
            'StorageClass': bucket.storage_class,
            'VersioningEnabled': bucket.versioning_enabled
        }

    def _get_object_metadata(self, bucket_name, object_name):
        """
        Retrieves the object's metadata from Bolt / GS.
        :param bucket_name: bucket name
        :param object_name: object name
        :return: object metadata
        """
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.get_blob(object_name)

        blob_md = {
            'ContentEncoding': blob.content_encoding,
            'ETag': blob.etag,
            'Md5Hash': blob.md5_hash,
            'Size': blob.size,
            'StorageClass': blob.storage_class,
            'TimeCreated': blob.time_created,
            'Updated': blob.updated
        }

        if blob.retention_expiration_time:
            blob_md['RetentionExpirationTime'] = blob.retention_expiration_time

        return blob_md

    def _upload_object(self, bucket_name, object_name, value):
        """
        Uploads an object to Bolt/GS.
        :param bucket_name: bucket name
        :param object_name: object name
        :param value: object data
        :return: object metadata
        """
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string(value)

        return {
            'ETag': blob.etag,
            'Md5Hash': blob.md5_hash
        }

    def _download_object(self, bucket_name, object_name):
        """
        Gets the object from Bolt/GS, computes and returns the object's MD5 hash
        If the object is gzip encoded, object is decompressed before computing its MD5.
        :param bucket_name: bucket name
        :param object_name: object name
        :return: md5 hash of the object.
        """
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.get_blob(object_name)
        blob_data = blob.download_as_bytes()

        if blob.content_encoding == "gzip" or str(object_name).endswith('.gz'):
            md5 = hashlib.md5(gzip.decompress(blob_data)).hexdigest().upper()
        else:
            md5 = hashlib.md5(blob_data).hexdigest().upper()

        return {
            'md5': md5
        }

    def _delete_object(self, bucket_name, object_name):
        """

        :param bucket_name: bucket name
        :param object_name: object name
        :return: Deleted Status
        """
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.delete()

        return {
            'Deleted': 'True'
        }

    @staticmethod
    def get_region():
        """
        Get Deployment region of the function
        :return: region
        """
        md_zone_url = 'http://metadata.google.internal/computeMetadata/v1/instance/zone'
        headers = {'Metadata-Flavor': 'Google'}
        r = requests.get(md_zone_url, headers=headers)

        zone = r.text.split('/')[-1]
        region = zone.rsplit('-', 1)[0]
        return region

    def validate_obj_md5(self, request):
        """
        validate_obj_md5 retrieves the object from Bolt and GS (if BucketClean is OFF), computes and
        returns their corresponding MD5 hash. If the object is gzip encoded, object is decompressed before
        computing its MD5.
        :param request: request object
        :return: md5s of object retrieved from Bolt and GS
        """
        request_json = request.get_json()

        if request_json:
            if 'bucket' in request_json:
                bucket_name = request_json['bucket']

            if 'bucketClean' in request_json:
                bucket_clean = str(request_json['bucketClean']).upper()
            else:
                bucket_clean = 'OFF'

            if 'key' in request_json:
                object_name = request_json['key']

        gs_storage_client = storage.Client()
        client_options = {"api_endpoint": self._bolt_url}
        bolt_storage_client = storage.Client(client_options=client_options)

        try:
            # Get Object from Bolt.
            bolt_bucket = bolt_storage_client.bucket(bucket_name)
            bolt_blob = bolt_bucket.get_blob(object_name)
            bolt_blob_data = bolt_blob.download_as_bytes()

            # Get Object from GS if bucket clean is off
            if bucket_clean == 'OFF':
                gs_bucket = gs_storage_client.bucket(bucket_name)
                gs_blob = gs_bucket.get_blob(object_name)
                gs_blob_data = gs_blob.download_as_bytes()

            # Parse the MD5 of the returned object.
            # If Object is gzip encoded, compute MD5 on the decompressed object.
            if gs_blob.content_encoding == "gzip" or str(object_name).endswith('.gz'):
                bolt_md5 = hashlib.md5(gzip.decompress(bolt_blob_data)).hexdigest().upper()
                gs_md5 = hashlib.md5(gzip.decompress(gs_blob_data)).hexdigest().upper()
            else:
                bolt_md5 = hashlib.md5(bolt_blob_data).hexdigest().upper()
                gs_md5 = hashlib.md5(gs_blob_data).hexdigest().upper()

            return {
                'gs-md5': gs_md5,
                'bolt-md5': bolt_md5
            }
        except Exception as e:
            return {
                'errorMessage': str(e),
                'errorCode': str(1)
            }

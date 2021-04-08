import time
import os
from google.cloud import storage
from BoltGSOpsClient import BoltGSOpsClient


class BoltAutoHeal:
    """
    BoltAutoHeal processes Http Requests that are received by the function
    bolt_auto_heal_handler.
    """

    def __init__(self):
        # create bolt storage client.
        self._bolt_url = os.environ.get("BOLT_URL").replace('{region}', BoltGSOpsClient.get_region())
        client_options = {"api_endpoint": self._bolt_url}
        self._bolt_storage_client = storage.Client(client_options=client_options)

    def process_event(self, request):
        """
        process_event extracts the parameters (bucket, key) from the HTTP Request, uses those
        parameters to perform auto-heal test.

        :param request: request object
        :return: time taken to auto-heal
        """
        # Parse JSON Request.
        request_json = request.get_json()

        if request_json:
            if 'bucket' in request_json:
                bucket_name = request_json['bucket']

            if 'key' in request_json:
                object_name = request_json['key']

        return self.get_blob_until_success(bucket_name, object_name)

    def get_blob_until_success(self, bucket_name, object_name):
        """
        get_blob_until_success attempts to retrieve object repeatedly until it succeeds,
        which would indicate successful auto-healing of the object.

        :param bucket_name: bucket name
        :param object_name: object name
        :return: time taken to auto-heal
        """
        auto_heal_start_time = time.time()
        while True:
            try:
                # Get blob from Bolt.
                bucket = self._bolt_storage_client.bucket(bucket_name)
                blob = bucket.get_blob(object_name)
                # read the entire object.
                blob_data = blob.download_as_bytes()
                # exit on success after auto-heal
                auto_heal_end_time = time.time()
                break
            except Exception as e:
                pass

        auto_heal_time = auto_heal_end_time - auto_heal_start_time

        return {
            'auto_heal_time': "{:.2f} secs".format(auto_heal_time)
        }

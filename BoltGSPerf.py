import os
import time
import random
import string
import json
import math
from statistics import mean
from statistics import median_low
from google.cloud import storage
from BoltGSOpsClient import BoltGSOpsClient


class BoltGSPerf:

    # constants for PUT/DELETE Object Perf
    # max. no of keys to be used in
    NUM_KEYS = 1000
    # length of object data
    OBJ_LENGTH = 100

    def __init__(self):
        # create google storage client.
        self._gs_storage_client = storage.Client()
        # create bolt storage client.
        self._bolt_url = os.environ.get("BOLT_URL").replace('{region}', BoltGSOpsClient.get_region())
        client_options = {"api_endpoint": self._bolt_url}
        self._bolt_storage_client = storage.Client(client_options=client_options)
        # list of keys to be used in Ops.
        self._keys = None
        # request type
        self._request_type = None
        # Bolt/GS Ops latencies
        self._bolt_op_times = []
        self._gs_op_times = []
        # Bolt/GS Ops throughput
        self._bolt_op_tp = []
        self._gs_op_tp = []
        # Bolt/GS Obj sizes
        self._bolt_obj_sizes = []
        self._gs_obj_sizes = []
        # Bolt/GS object counts (compressed, uncompressed).
        self._gs_cmp_obj_count = 0
        self._gs_uncmp_obj_count = 0
        self._bolt_cmp_obj_count = 0
        self._bolt_uncmp_obj_count = 0

    def process_event(self, request):

        # Parse JSON Request.
        request_json = request.get_json()

        if request_json:
            if 'bucket' in request_json:
                bucket_name = request_json['bucket']

            if 'requestType' in request_json:
                self._request_type = str(request_json['requestType']).upper()
            else:
                self._request_type = 'ALL'

            # update max. no of keys and object data length, if passed in input.
            if 'numKeys' in request_json:
                self.NUM_KEYS = int(request_json['numKeys'])
                if self.NUM_KEYS > 1000:
                    self.NUM_KEYS = 1000
            if 'objLength' in request_json:
                self.OBJ_LENGTH = int(request_json['objLength'])

            # if keys not passed as in input:
            # if DOWNLOAD_OBJECT or DOWNLOAD_OBJECT_PASSTHROUGH, list objects (up to NUM_KEYS) to get key names
            # otherwise generate key names.
            if 'keys' in request_json:
                self._keys = request_json['keys']
            elif self._request_type == "DOWNLOAD_OBJECT" or self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH" or\
                    self._request_type == "DOWNLOAD_OBJECT_TTFB" or\
                    self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH_TTFB":
                self._keys = self._list_objects(request_json['bucket'])
            else:
                self._keys = self._generate_key_names(self.NUM_KEYS)

        # Perform Perf tests based on input 'requestType'
        try:
            if self._request_type == "LIST_OBJECTS":
                return self._list_objects_perf(bucket_name)
            elif self._request_type == "DOWNLOAD_OBJECT" or self._request_type == "DOWNLOAD_OBJECT_TTFB":
                return self._download_object_perf(bucket_name)
            elif self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH" or\
                    self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH_TTFB":
                return self._download_object_passthrough_perf(bucket_name)
            elif self._request_type == "UPLOAD_OBJECT":
                return self._upload_object_perf(bucket_name)
            elif self._request_type == "DELETE_OBJECT":
                return self._delete_object_perf(bucket_name)
            elif self._request_type == "ALL":
                return self._all_perf(bucket_name)
        except Exception as e:
            return {
                'errorMessage': str(e),
                'errorCode': str(1)
            }

    def _list_objects_perf(self, bucket_name, num_iter=10):

        # list 1000 objects from Bolt / GS, num_iter times.
        for x in range(num_iter):
            # list 1000 objects from GS.
            list_objects_start_time = time.time()
            gs_blobs = self._gs_storage_client.list_blobs(bucket_name, max_results=1000)
            list_objects_end_time = time.time()
            # calc latency
            list_objects_time = list_objects_end_time - list_objects_start_time
            self._gs_op_times.append(list_objects_time)
            # calc throughput
            num_blobs = len(list(gs_blobs))
            list_objects_tp = num_blobs / list_objects_time
            self._gs_op_tp.append(list_objects_tp)

            # list 1000 objects from Bolt.
            list_objects_start_time = time.time()
            bolt_blobs = self._bolt_storage_client.list_blobs(bucket_name, max_results=1000)
            list_objects_end_time = time.time()
            # calc latency
            list_objects_time = list_objects_end_time - list_objects_start_time
            self._bolt_op_times.append(list_objects_time)
            # calc throughput
            num_blobs = len(list(bolt_blobs))
            list_objects_tp = num_blobs / list_objects_time
            self._bolt_op_tp.append(list_objects_tp)

        # calc gs perf stats.
        gs_list_objects_perf_stats = self._compute_perf_stats(self._gs_op_times, self._gs_op_tp)

        # calc bolt perf stats.
        bolt_list_objects_perf_stats = self._compute_perf_stats(self._bolt_op_times, self._bolt_op_tp)

        list_objects_perf_stats = {
            'gs_list_objs_perf_stats': gs_list_objects_perf_stats,
            'bolt_list_objs_perf_stats': bolt_list_objects_perf_stats
        }
        if self._request_type == "ALL":
            return list_objects_perf_stats
        else:
            return json.dumps(list_objects_perf_stats, indent=4, sort_keys=True)

    def _download_object_perf(self, bucket_name):
        # Get blobs from Bolt/GS.
        for key in self._keys:
            # Get blob from GS.
            bucket = self._gs_storage_client.bucket(bucket_name)
            blob = bucket.get_blob(key)
            if blob.size > 0:
                obj_download_start_time = time.time()
                if self._request_type == "DOWNLOAD_OBJECT_TTFB":
                    # get first byte of object
                    blob.download_as_bytes(start=0, end=0)
                else:
                    # read the entire object.
                    blob.download_as_bytes()
                obj_download_end_time = time.time()
                # calc latency
                download_obj_time = obj_download_end_time - obj_download_start_time
                self._gs_op_times.append(download_obj_time)
                # count object
                if blob.content_encoding == "gzip" or str(key).endswith('.gz'):
                    self._gs_cmp_obj_count += 1
                else:
                    self._gs_uncmp_obj_count += 1
                # get blob size
                self._gs_obj_sizes.append(blob.size)

            # Get blob from Bolt.
            bucket = self._bolt_storage_client.bucket(bucket_name)
            blob = bucket.get_blob(key)
            if blob.size > 0:
                obj_download_start_time = time.time()
                if self._request_type == "DOWNLOAD_OBJECT_TTFB":
                    # get first byte of object
                    blob.download_as_bytes(start=0, end=0)
                else:
                    # read the entire object.
                    blob.download_as_bytes()
                obj_download_end_time = time.time()
                # calc latency
                download_obj_time = obj_download_end_time - obj_download_start_time
                self._bolt_op_times.append(download_obj_time)
                # count object
                if blob.content_encoding == "gzip" or str(key).endswith('.gz'):
                    self._bolt_cmp_obj_count += 1
                else:
                    self._bolt_uncmp_obj_count += 1
                # get blob size
                self._bolt_obj_sizes.append(blob.size)

        # calc gs perf stats
        gs_download_obj_perf_stats = self._compute_perf_stats(self._gs_op_times, obj_sizes=self._gs_obj_sizes)

        # calc bolt perf stats
        bolt_download_obj_perf_stats = self._compute_perf_stats(self._bolt_op_times, obj_sizes=self._bolt_obj_sizes)

        # assign perf stats name
        if self._request_type == "DOWNLOAD_OBJECT_TTFB":
            gs_dwnld_obj_stat_name = 'gs_download_obj_ttfb_perf_stats'
            bolt_dwnld_obj_stat_name = 'bolt_download_obj_ttfb_perf_stats'
        else:
            gs_dwnld_obj_stat_name = 'gs_download_obj_perf_stats'
            bolt_dwnld_obj_stat_name = 'bolt_download_obj_perf_stats'

        download_obj_perf_stats = {
            gs_dwnld_obj_stat_name: gs_download_obj_perf_stats,
            'gs_object_count (compressed)': self._gs_cmp_obj_count,
            'gs_object_count (uncompressed)': self._gs_uncmp_obj_count,
            bolt_dwnld_obj_stat_name: bolt_download_obj_perf_stats,
            'bolt_object_count (compressed)': self._bolt_cmp_obj_count,
            'bolt_object_count (uncompressed)': self._bolt_uncmp_obj_count,
        }
        if self._request_type == "ALL":
            return download_obj_perf_stats
        else:
            return json.dumps(download_obj_perf_stats, indent=4, sort_keys=True)

    def _download_object_passthrough_perf(self, bucket_name):
        # Get Objects via passthrough from Bolt.
        for key in self._keys:
            # Get blob from Bolt.
            bucket = self._bolt_storage_client.bucket(bucket_name)
            blob = bucket.get_blob(key)
            if blob.size > 0:
                obj_download_start_time = time.time()
                if self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH_TTFB":
                    # get first byte of object
                    blob.download_as_bytes(start=0, end=0)
                else:
                    # read the entire object.
                    blob.download_as_bytes()
                obj_download_end_time = time.time()
                # calc latency
                download_obj_time = obj_download_end_time - obj_download_start_time
                self._bolt_op_times.append(download_obj_time)
                # count object
                if blob.content_encoding == "gzip" or str(key).endswith('.gz'):
                    self._bolt_cmp_obj_count += 1
                else:
                    self._bolt_uncmp_obj_count += 1
                # get blob size
                self._bolt_obj_sizes.append(blob.size)

        # calc bolt perf stats
        bolt_dwnld_obj_pt_perf_stats = self._compute_perf_stats(self._bolt_op_times, obj_sizes=self._bolt_obj_sizes)

        # assign perf stats name.
        if self._request_type == "DOWNLOAD_OBJECT_PASSTHROUGH_TTFB":
            bolt_dwnld_obj_pt_stat_name = 'bolt_download_obj_pt_ttfb_perf_stats'
        else:
            bolt_dwnld_obj_pt_stat_name = 'bolt_download_obj_pt_perf_stats'

        download_obj_pt_perf_stats = {
            bolt_dwnld_obj_pt_stat_name: bolt_dwnld_obj_pt_perf_stats,
            'bolt_object_count (compressed)': self._bolt_cmp_obj_count,
            'bolt_object_count (uncompressed)': self._bolt_uncmp_obj_count
        }
        if self._request_type == "ALL":
            return download_obj_pt_perf_stats
        else:
            return json.dumps(download_obj_pt_perf_stats, indent=4, sort_keys=True)

    def _upload_object_perf(self, bucket_name):
        # Upload objects to Bolt/GS.
        for key in self._keys:
            value = self._generate(characters=string.ascii_lowercase, length=self.OBJ_LENGTH)

            # upload object to GS.
            bucket = self._gs_storage_client.bucket(bucket_name)
            blob = bucket.blob(key)
            obj_upload_start_time = time.time()
            blob.upload_from_string(value)
            obj_upload_end_time = time.time()
            # calc latency
            obj_upload_time = obj_upload_end_time - obj_upload_start_time
            self._gs_op_times.append(obj_upload_time)

            # upload object to Bolt.
            bucket = self._bolt_storage_client.bucket(bucket_name)
            blob = bucket.blob(key)
            obj_upload_start_time = time.time()
            blob.upload_from_string(value)
            obj_upload_end_time = time.time()
            # calc latency
            obj_upload_time = obj_upload_end_time - obj_upload_start_time
            self._bolt_op_times.append(obj_upload_time)

        # calc GS perf stats
        gs_upload_obj_perf_stats = self._compute_perf_stats(self._gs_op_times)

        # calc bolt perf stats
        bolt_upload_obj_perf_stats = self._compute_perf_stats(self._bolt_op_times)

        upload_obj_perf_stats = {
            'object_size': "{:d} bytes".format(self.OBJ_LENGTH),
            'gs_upload_obj_perf_stats': gs_upload_obj_perf_stats,
            'bolt_upload_obj_perf_stats': bolt_upload_obj_perf_stats
        }
        if self._request_type == "ALL":
            return upload_obj_perf_stats
        else:
            return json.dumps(upload_obj_perf_stats, indent=4, sort_keys=True)

    def _delete_object_perf(self, bucket_name):
        # Delete Objects from Bolt/GS.
        for key in self._keys:
            # Delete blob from GS.
            bucket = self._gs_storage_client.bucket(bucket_name)
            blob = bucket.blob(key)
            obj_del_start_time = time.time()
            blob.delete()
            obj_del_end_time = time.time()
            # calc latency.
            obj_del_time = obj_del_end_time - obj_del_start_time
            self._gs_op_times.append(obj_del_time)

            # Delete blob from Bolt.
            bucket = self._bolt_storage_client.bucket(bucket_name)
            blob = bucket.blob(key)
            obj_del_start_time = time.time()
            blob.delete()
            obj_del_end_time = time.time()
            # calc latency.
            obj_del_time = obj_del_end_time - obj_del_start_time
            self._bolt_op_times.append(obj_del_time)

        # calc s3 perf stats
        gs_del_obj_perf_stats = self._compute_perf_stats(self._gs_op_times)

        # calc bolt perf stats
        bolt_del_obj_perf_stats = self._compute_perf_stats(self._bolt_op_times)

        del_obj_perf_stats = {
            'gs_del_obj_perf_stats': gs_del_obj_perf_stats,
            'bolt_del_obj_perf_stats': bolt_del_obj_perf_stats
        }
        if self._request_type == "ALL":
            return del_obj_perf_stats
        else:
            return json.dumps(del_obj_perf_stats, indent=4, sort_keys=True)

    def _all_perf(self, bucket_name):
        # Upload / Delete Objects using generated key names.
        upload_obj_perf_stats = self._upload_object_perf(bucket_name)
        self._clear_stats()
        del_obj_perf_stats = self._delete_object_perf(bucket_name)
        self._clear_stats()

        # LIST Objects.
        list_objs_perf_stats = self._list_objects_perf(bucket_name)
        self._clear_stats()

        # Get the list of objects before download_obj_perf_test.
        self._keys = self._list_objects(bucket_name)
        download_obj_perf_stats = self._download_object_perf(bucket_name)

        all_perf_stats = self._merge_perf_stats(upload_obj_perf_stats,
                                                download_obj_perf_stats,
                                                del_obj_perf_stats,
                                                list_objs_perf_stats)
        return json.dumps(all_perf_stats, indent=4, sort_keys=True)

    def _merge_perf_stats(self, *perf_stats):
        """
        Merge one or more dictionaries containing
        performance statistics into one dictionary.
        :param perf_stats: one or more performance statistics
        :return: merged performance statistics
        """
        merged_perf_stats = {}
        for perf_stat in perf_stats:
            merged_perf_stats.update(perf_stat)
        return merged_perf_stats

    def _compute_perf_stats(self, op_times, op_tp=None, obj_sizes=None):
        """
        Compute performance statistics
        :param op_times: list of latencies
        :param op_tp: list of throughputs
        :param obj_sizes: list of object sizes
        :return: performance statistics (latency, throughput, object size)
        """
        # calc op latency perf.
        op_avg_time = mean(op_times)
        op_time_p50 = median_low(op_times)
        op_times.sort()
        p90_index = int(len(op_times) * 0.9)
        op_time_p90 = op_times[p90_index]

        # calc op throughout perf.
        if op_tp:
            op_avg_tp = mean(op_tp)
            op_tp_p50 = median_low(op_tp)
            op_tp.sort()
            p90_index = int(len(op_tp) * 0.9)
            op_tp_p90 = op_tp[p90_index]
            tp_perf_stats = {
                'average': "{:.2f} objects/sec".format(op_avg_tp),
                'p50': "{:.2f} objects/sec".format(op_tp_p50),
                'p90': "{:.2f} objects/sec".format(op_tp_p90)
            }
        else:
            tp = len(op_times) / math.fsum(op_times)
            tp_perf_stats = "{:.2f} objects/sec".format(tp)

        # calc obj size metrics.
        if obj_sizes:
            obj_avg_size = mean(obj_sizes)
            obj_sizes_p50 = median_low(obj_sizes)
            obj_sizes.sort()
            p90_index = int(len(obj_sizes) * 0.9)
            obj_sizes_p90 = obj_sizes[p90_index]
            obj_sizes_perf_stats = {
                'average': "{:.2f} bytes".format(obj_avg_size),
                'p50': "{:.2f} bytes".format(obj_sizes_p50),
                'p90': "{:.2f} bytes".format(obj_sizes_p90)
            }

        perf_stats = {
            'latency': {
                'average': "{:.2f} secs".format(op_avg_time),
                'p50': "{:.2f} secs".format(op_time_p50),
                'p90': "{:.2f} secs".format(op_time_p90)
            },
            'throughput': tp_perf_stats
        }
        if obj_sizes:
            perf_stats['object_size'] = obj_sizes_perf_stats

        return perf_stats

    def _clear_stats(self):
        self._bolt_op_times.clear()
        self._gs_op_times.clear()
        self._bolt_op_tp.clear()
        self._gs_op_tp.clear()
        self._bolt_obj_sizes.clear()
        self._gs_obj_sizes.clear()
        self._gs_cmp_obj_count = 0
        self._gs_uncmp_obj_count = 0
        self._bolt_cmp_obj_count = 0
        self._bolt_uncmp_obj_count = 0

    def _generate_key_names(self, num_objects):
        """
        Generate Object names to be used in PUT/GET Object operations.
        :param num_objects: number of objects
        :return: list of object names
        """
        objects = []
        for x in range(num_objects):
            obj_name = 'bolt-gs-perf' + str(x)
            objects.append(obj_name)
        return objects

    def _generate(self, characters=string.ascii_lowercase, length=10):
        """
        Generate a random string of certain length
        :param characters: character set to be used
        :param length: length of the string.
        :return: generated string
        """
        return ''.join(random.choice(characters) for _ in range(length))

    def _list_objects(self, bucket_name):
        """
        Returns a list of 1000 objects from the given bucket in Bolt/GS
        :param bucket_name: bucket name
        :return: list of 1000 objects
        """
        blobs = self._gs_storage_client.list_blobs(bucket_name, max_results=1000)
        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)

        return blob_names

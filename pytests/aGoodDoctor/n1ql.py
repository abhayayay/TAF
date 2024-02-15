'''
Created on 24-Apr-2021

@author: riteshagarwal
'''

import json
import random
from threading import Thread
import threading
import time

from common_lib import sleep
from sdk_client3 import SDKClient
from com.couchbase.client.java.query import QueryOptions,\
    QueryScanConsistency, QueryStatus
from com.couchbase.client.core.deps.io.netty.handler.timeout import TimeoutException
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, InternalServerFailureException,\
    AmbiguousTimeoutException
from string import ascii_uppercase, ascii_lowercase
from encodings.punycode import digits
from remote.remote_util import RemoteMachineShellConnection
from gsiLib.gsiHelper import GsiHelper
import traceback
from global_vars import logger

letters = ascii_uppercase + ascii_lowercase + digits

queries = ['select name from {} where age between 30 and 50 limit 10;',
           'select age, count(*) from {} where marital = "M" group by age order by age limit 10;',
           'select v.name, animal from {} as v unnest animals as animal where v.attributes.hair = "Burgundy" limit 10;',
           'SELECT v.name, ARRAY hobby.name FOR hobby IN v.attributes.hobbies END FROM {} as v WHERE v.attributes.hair = "Burgundy" and gender = "F" and ANY hobby IN v.attributes.hobbies SATISFIES hobby.type = "Music" END limit 10;',
           'select name, ROUND(attributes.dimensions.weight / attributes.dimensions.height,2) from {} WHERE gender is not MISSING limit 10;']
indexes = ['create index {}{} on {}(age) where age between 30 and 50 WITH {{ "defer_build": true, "num_replica": 0 }};',
           'create index {}{} on {}(marital,age) WITH {{ "defer_build": true, "num_replica": 0 }};',
           'create index {}{} on {}(ALL `animals`,`attributes`.`hair`,`name`) where attributes.hair = "Burgundy" WITH {{ "defer_build": true, "num_replica": 0 }};',
           'CREATE INDEX {}{} ON {}(`gender`,`attributes`.`hair`, DISTINCT ARRAY `hobby`.`type` FOR hobby in `attributes`.`hobbies` END) where gender="F" and attributes.hair = "Burgundy" WITH {{ "defer_build": true, "num_replica": 0 }};',
           'create index {}{} on {}(`gender`,`attributes`.`dimensions`.`weight`, `attributes`.`dimensions`.`height`,`name`) WITH {{ "defer_build": true, "num_replica": 0 }};']


class DoctorN1QL():

    def __init__(self, cluster, bucket_util, num_idx=10,
                 server_port=8095,
                 querycount=100, batch_size=50, num_query=10, query_without_index=False):
        self.port = server_port
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.total_query_count = 0
        self.concurrent_batch_size = batch_size
        self.total_count = querycount
        self.num_indexes = num_idx
        # num_query is used when queries are run without index creation
        self.num_query = num_query
        self.bucket_util = bucket_util
        self.cluster = cluster
        self.sdkClients = dict()
        self.log = logger.get("test")

        self.sdkClient = SDKClient(cluster, None, servers=cluster.query_nodes)
        self.cluster_conn = self.sdkClient.cluster
        self.stop_run = False
        self.query_failure = False
        self.queries = list()
        i = 0
        if query_without_index:
            while i < self.num_query:
                for b in self.cluster.buckets:
                    for s in self.bucket_util.get_active_scopes(b, only_names=True):
                        if b.name+s not in self.sdkClients.keys():
                            self.sdkClients.update({b.name+s: self.cluster_conn.bucket(b.name).scope(s)})
                        for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                            if c == "_default":
                                continue
                            self.queries.append((queries[i % len(queries)].format(c), self.sdkClients[b.name+s]))
                            i+=1
                            if i >= self.num_query:
                                break
                        if i >= self.num_query:
                            break
                    if i >= self.num_query:
                        break
        self.indexes = dict()
        while i < self.num_indexes:
            for b in self.cluster.buckets:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    if b.name+s not in self.sdkClients.keys():
                        self.sdkClients.update({b.name+s: self.cluster_conn.bucket(b.name).scope(s)})
                    for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                        self.idx_q = indexes[i % len(indexes)].format("idx", i, c)
                        self.indexes.update({"idx"+str(i): (self.idx_q, self.sdkClients[b.name+s], b.name, s, c)})
                        self.queries.append((queries[i % len(indexes)].format(c), self.sdkClients[b.name+s]))
                        i += 1
                        if i >= self.num_indexes:
                            break
                    if i >= self.num_indexes:
                        break
                if i >= self.num_indexes:
                    break

    def discharge_N1QL(self):
        self.stop_run = True
    def query_result(self):
        return self.query_failure

    def create_indexes(self):
        for details in self.indexes.values():
            time.sleep(1)
            self.execute_statement_on_n1ql(details[1], details[0])

    def wait_for_indexes_online(self, logger, indexes, timeout=86400):
        self.rest = GsiHelper(self.cluster.master, logger)
        status = False
        for index_name, details in indexes.items():
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                bucket = [bucket for bucket in self.cluster.buckets if bucket.name == details[2]]
                status = self.rest.polling_create_index_status(bucket[0], index_name)
                print("index: {}, status: {}".format(index_name, status))
                if status is True:
                    self.log.info("2i index is ready: {}".format(index_name))
                    break
                time.sleep(5)
            if status is False:
                return status
        return status

    def build_indexes(self):
        for index_name, details in self.indexes.items():
            build_query = "BUILD INDEX on `%s`(%s) USING GSI" % (details[4], index_name)
            time.sleep(1)
            try:
                self.execute_statement_on_n1ql(details[1], build_query)
            except Exception as e:
                print(e)
                print("Failed %s" % build_query)

    def drop_indexes(self):
        for index, details in self.indexes.items():
            build_query = "DROP INDEX %s on `%s`" % (index, details[4])
            self.execute_statement_on_n1ql(details[1], build_query)

    def start_query_load(self):
        th = threading.Thread(target=self._run_concurrent_queries,
                              kwargs=dict(num_queries=self.num_indexes))
        th.start()

        monitor = threading.Thread(target=self.monitor_query_status,
                                   kwargs=dict(duration=0,
                                               print_duration=60))
        monitor.start()

    def _run_concurrent_queries(self, num_queries):
        threads = []
        self.total_query_count = 0
        query_count = 0
        for i in range(0, num_queries):
            self.total_query_count += 1
            query = random.choice(self.queries)
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.total_query_count),
                args=(query[1], query[0], False, 0)))

        i = 0
        for thread in threads:
            i += 1
            if i % self.concurrent_batch_size == 0:
                time.sleep(5)
            thread.start()
            query_count += 1

        i = 0
        while not self.stop_run:
            threads = []
            new_queries_to_run = self.total_count - num_queries
            for i in range(0, new_queries_to_run):
                query = random.choice(self.queries)
                self.total_query_count += 1
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(self.total_query_count),
                    args=(query[1], query[0], False, 0)))
            i = 0
            self.total_count += new_queries_to_run
            for thread in threads:
                i += 1
                thread.start()

            time.sleep(2)
        if self.failed_count + self.error_count != 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" %
                            (self.failed_count, self.error_count))

    def run_concurrent_queries(self, num_queries):
        self.query_failure = False
        while not self.stop_run:
            threads = []
            self.total_query_count = 0
            self.failed_count = 0
            self.error_count = 0

            for i in range(0, num_queries):
                self.total_query_count += 1
                query = random.choice(self.queries)
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(self.total_query_count),
                    args=(query[1], query[0], False, 0)))
            i = 0
            for thread in threads:
                i += 1
                if i % self.concurrent_batch_size == 0:
                    time.sleep(5)
                thread.start()

            time.sleep(2)
            if self.failed_count + self.error_count != 0:
                self.stop_run = True
                self.query_failure = True
                msg = "Queries Failed:{} , Queries Error Out:{}".format
                (self.failed_count, self.error_count)
                self.log.critical(msg)

    def _run_query(self, client, query, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        client_context_id = name
        try:
            status, _, _, results, _ = self.execute_statement_on_n1ql(
                client, query, client_context_id=client_context_id)
            if status == QueryStatus.SUCCESS:
                if validate_item_count:
                    if results[0]['$1'] != expected_count:
                        self.failed_count += 1
                        self.total_count -= 1
                    else:
                        self.success_count += 1
                        self.total_count -= 1
                else:
                    self.success_count += 1
                    self.total_count -= 1
            else:
                self.failed_count += 1
                self.total_count -= 1
        except Exception as e:
            if e == TimeoutException or e == AmbiguousTimeoutException:
                self.timeout_count += 1
                self.total_count -= 1
            elif e == RequestCanceledException:
                self.cancel_count += 1
                self.total_count -= 1
            elif e == CouchbaseException:
                self.rejected_count += 1
                self.total_count -= 1
            else:
                self.error_count += 1
                self.total_count -= 1

    def execute_statement_on_n1ql(self, client, statement, client_context_id=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        try:
            response = self.execute_via_sdk(client, statement, False, client_context_id)
            if type(response) == str:
                response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
            else:
                errors = None

            if "results" in response:
                results = response["results"]
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None

            if "metrics" in response:
                metrics = response["metrics"]
            else:
                metrics = None
            if "status" in response:
                status = response["status"]
            else:
                status = None
            return status, metrics, errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def execute_via_sdk(self, client, statement, readonly=False,
                        client_context_id=None):
        options = QueryOptions.queryOptions()
        options.scanConsistency(QueryScanConsistency.NOT_BOUNDED)
        options.readonly(readonly)
        if client_context_id:
            options.clientContextId(client_context_id)

        output = {}
        try:
            result = client.query(statement)

            output["status"] = result.metaData().status()
            output["metrics"] = result.metaData().metrics()

            try:
                output["results"] = result.rowsAsObject()
            except:
                output["results"] = None

            if str(output['status']) == QueryStatus.FATAL:
                msg = output['errors'][0]['msg']
                if "Job requirement" in msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif output['status'] == QueryStatus.SUCCESS:
                output["errors"] = None
            else:
                raise Exception("N1QL query failed")

        except InternalServerFailureException as e:
            print(e)
            traceback.print_exc()
            raise Exception(e)
        except TimeoutException | AmbiguousTimeoutException as e:
            raise Exception(e)
        except RequestCanceledException as e:
            raise Exception(e)
        except CouchbaseException as e:
            raise Exception(e)
        except Exception as e:
            print(e)
            traceback.print_exc()
        return output

    def monitor_query_status(self, duration=0, print_duration=600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    print("%s N1QL queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout, %s errored" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count,
                        self.error_count))
                    st_time = time.time()
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    print("%s N1QL queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout, %s errored" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count,
                        self.error_count))
                    update_time = time.time()

    def crash_index_plasma(self, nodes=None):
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.index_nodes
        shells = list()
        for node in nodes:
            shells.append(RemoteMachineShellConnection(node))
        while not self.stop_run:
            sleep = random.randint(120, 240)
            self.sleep(sleep,
                       "Iteration:{} waiting to kill indexer on nodes: {}".format(self.crash_count, nodes))
            for shell in shells:
                shell.kill_indexer()
            self.crash_count += 1
            if self.crash_count > self.crashes:
                break
        for shell in shells:
            shell.disconnect()
        self.sleep(300)

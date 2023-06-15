'''
Created on May 2, 2022

@author: ritesh.agarwal
'''
import random

from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
import threading
from aGoodDoctor.bkrs import DoctorBKRS
import os
from BucketLib.bucket import Bucket
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from aGoodDoctor.hostedFTS import DoctorFTS, FTSQueryLoad
from aGoodDoctor.hostedN1QL import QueryLoad, DoctorN1QL
from aGoodDoctor.hostedCbas import DoctorCBAS, CBASQueryLoad
from aGoodDoctor.hostedXDCR import DoctorXDCR
from aGoodDoctor.hostedBackupRestore import DoctorHostedBackupRestore
from aGoodDoctor.hostedEventing import DoctorEventing
from aGoodDoctor.hostedOPD import OPD
from constants.cloud_constants.capella_constants import AWS, GCP
from table_view import TableView
import time
from Cb_constants.CBServer import CbServer
from bucket_utils.bucket_ready_functions import CollectionUtils
from com.couchbase.test.sdk import Server
from constants.cloud_constants import capella_constants


class Murphy(BaseTestCase, OPD):

    def init_doc_params(self):
        self.create_perc = self.input.param("create_perc", 100)
        self.update_perc = self.input.param("update_perc", 20)
        self.delete_perc = self.input.param("delete_perc", 20)
        self.expiry_perc = self.input.param("expiry_perc", 20)
        self.read_perc = self.input.param("read_perc", 20)
        self.start = 0
        self.end = 0
        self.initial_items = self.start
        self.final_items = self.end
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0

    def setUp(self):
        BaseTestCase.setUp(self)
        self.init_doc_params()

        self.num_collections = self.input.param("num_collections", 1)
        self.xdcr_collections = self.input.param("xdcr_collections", self.num_collections)
        self.num_collections_bkrs = self.input.param("num_collections_bkrs", self.num_collections)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.xdcr_scopes = self.input.param("xdcr_scopes", self.num_scopes)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.rebalance_type = self.input.param("rebalance_type", "all")
        self.kv_nodes = self.nodes_init
        self.cbas_nodes = self.input.param("cbas_nodes", 0)
        self.fts_nodes = self.input.param("fts_nodes", 0)
        self.index_nodes = self.input.param("index_nodes", 0)
        self.eventing_nodes = self.input.param("eventing_nodes", 0)
        self.backup_nodes = self.input.param("backup_nodes", 0)
        self.xdcr_remote_clusters = self.input.param("xdcr_remote_clusters", 0)
        self.num_indexes = self.input.param("num_indexes", 0)
        self.mutation_perc = 100
        self.threads_calculation()
        self.op_type = self.input.param("op_type", "create")
        self.dgm = self.input.param("dgm", None)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.iterations = self.input.param("iterations", 10)
        self.step_iterations = self.input.param("step_iterations", 1)
        self.rollback = self.input.param("rollback", True)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.end_step = self.input.param("end_step", None)
        self.key_prefix = "Users"
        self.crashes = self.input.param("crashes", 20)
        self.check_dump_thread = True
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = self.input.param("track_failures", True)
        self.loader_dict = None
        self.parallel_reads = self.input.param("parallel_reads", False)
        self._data_validation = self.input.param("data_validation", True)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.gtm = self.input.param("gtm", False)
        self.cursor_dropping_checkpoint = self.input.param(
            "cursor_dropping_checkpoint", None)
        self.index_timeout = self.input.param("index_timeout", 3600)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        self.num_of_datasets = self.input.param("num_datasets", 10)
        self.load_defn = list()

        if self.cluster.eventing_nodes:
            self.drEventing = DoctorEventing(self.cluster, self.bucket_util)

        if self.cluster.index_nodes:
            self.drIndex = DoctorN1QL(self.cluster, self.bucket_util)

        if self.cluster.fts_nodes:
            self.drFTS = DoctorFTS(self.cluster, self.bucket_util)

        if self.cluster.cbas_nodes:
            self.drCBAS = DoctorCBAS(self.cluster, self.bucket_util)

        if self.backup_nodes > 0:
            self.drBackupRestore = DoctorHostedBackupRestore(cluster=self.cluster,
                                                             bucket_name=self.cluster.buckets[0].name,
                                                             pod=self.pod,
                                                             tenant=self.tenant)
        if self.xdcr_remote_clusters > 0:
            self.drXDCR = DoctorXDCR(source_cluster=self.cluster, destination_cluster=self.xdcr_cluster,
                                     source_bucket=self.cluster.buckets[0].name,
                                     destination_bucket=self.xdcr_cluster.buckets[0].name,
                                     pod=self.pod, tenant=self.tenant)

        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.sdk_client_pool = self.bucket_util.initialize_java_sdk_client_pool()

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        self.stop_run = True
        if self.cluster.query_nodes:
            for ql in self.ql:
                ql.stop_run = True
            self.drIndex.stop_run = True
        if self.cluster.fts_nodes:
            for ql in self.ftsQL:
                ql.stop_run = True
            self.drFTS.stop_run = True
        BaseTestCase.tearDown(self)

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        for bucket in buckets:
            self.log.info("Using SDK endpoint %s" % self.cluster.srv)
            server = Server(self.cluster.srv, self.cluster.master.port,
                            self.cluster.master.rest_username,
                            self.cluster.master.rest_password,
                            str(self.cluster.master.memcached_port))
            self.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
            bucket.clients = self.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

    def rebalance_config(self, num):
        initial_services = self.input.param("services", "data")
        services = self.input.param("rebl_services", initial_services)
        server_group_list = list()
        services_map = {"data": "data",
                        "kv": "data",
                        "index": "index",
                        "2i": "index",
                        "query": "query",
                        "n1ql": "query",
                        "analytics": "analytics",
                        "cbas": "analytics",
                        "search": "search",
                        "fts": "search",
                        "eventing": "eventing"}
        provider = self.input.param("provider", "aws").lower()

        _type = AWS.StorageType.GP3 if provider == "aws" else "pd-ssd"
        storage_type = self.input.param("type", _type).upper()
        for service_group in services.split("-"):
            grp_services = service_group.split(":")
            service = grp_services[0]
            config = {
                "size": self.num_nodes[service] + num,
                "services": [services_map[_service.lower()] for _service in grp_services],
                "compute": self.compute[service],
                "storage": {
                    "type": storage_type,
                    "size": self.disk[service],
                    "iops": self.input.param("iops", 3000)
                }
            }
            if self.capella_cluster_config.get("place"):
                    if self.capella_cluster_config["place"]["hosted"]["provider"] != "aws":
                        config["storage"].pop("iops")
            elif self.capella_cluster_config["provider"] != "hostedAWS":
                config["storage"].pop("iops")
            server_group_list.append(config)
        return server_group_list

    def create_buckets(self):
        self.PrintStep("Step 2: Create required buckets and collections.")
        self.log.info("Create CB buckets")
        # Create Buckets
        for cluster in [self.cluster, self.xdcr_cluster]:
            if cluster:
                self.log.info("Get the available memory quota")
                rest = RestConnection(cluster.master)
                self.info = rest.get_nodes_self()
                # threshold_memory_vagrant = 100
                kv_memory = int(self.info.memoryQuota*0.8)
                ramQuota = self.input.param("ramQuota", kv_memory)
                buckets = ["default"] * self.num_buckets
                bucket_type = self.bucket_type.split(';') * self.num_buckets
                for i in range(self.num_buckets):
                    bucket = Bucket(
                        {Bucket.name: buckets[i] + str(i),
                         Bucket.ramQuotaMB: ramQuota / self.num_buckets,
                         Bucket.maxTTL: self.bucket_ttl,
                         Bucket.replicaNumber: self.num_replicas,
                         Bucket.storageBackend: self.bucket_storage,
                         Bucket.evictionPolicy: self.bucket_eviction_policy,
                         Bucket.bucketType: bucket_type[i],
                         Bucket.durabilityMinLevel: self.bucket_durability_level,
                         Bucket.flushEnabled: True,
                         Bucket.fragmentationPercentage: self.fragmentation})
                    self.bucket_params = {
                        "name": bucket.name,
                        "bucketConflictResolution": "seqno",
                        "memoryAllocationInMb": bucket.ramQuotaMB,
                        "flush": bucket.flushEnabled,
                        "replicas": bucket.replicaNumber,
                        "storageBackend": bucket.storageBackend,
                        "durabilityLevel": bucket.durability_level,
                        "timeToLive": {"unit": "seconds", "value": bucket.maxTTL}
                    }
                    CapellaAPI.create_bucket(cluster, self.bucket_params)
                    self.bucket_util.get_updated_bucket_server_list(cluster, bucket)
                    bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                    cluster.buckets.append(bucket)
                if not cluster == self.xdcr_cluster:
                    self.buckets = cluster.buckets
                    num_clients = self.input.param("clients_per_db",
                                                   min(5, bucket.loadDefn.get("collections")))
                    for bucket in cluster.buckets:
                        self.create_sdk_client_pool([bucket],
                                                    num_clients)
                self.create_required_collections(cluster)

    def restart_query_load(self):
        for ql in self.ql:
            ql.stop_query_load()
        self.sleep(10)
        self.ql = list()
        for bucket in self.cluster.buckets:
            if bucket.loadDefn.get("2iQPS", 0) > 0:
                # bucket.loadDefn.get("2i")[1] = bucket.loadDefn.get("2i")[1] + 10
                ql = QueryLoad(bucket)
                ql.start_query_load()
                self.ql.append(ql)

    def monitor_query_status(self, print_duration=120):

        def check_query_stats():
            st_time = time.time()
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    self.query_table = TableView(self.log.info)
                    self.table = TableView(self.log.info)
                    self.table.set_headers(["Bucket",
                                            "Total Queries",
                                            "Failed Queries",
                                            "Success Queries",
                                            "Rejected Queries",
                                            "Cancelled Queries",
                                            "Timeout Queries",
                                            "Errored Queries"])
                    for ql in self.ql:
                        self.query_table.set_headers(["Bucket",
                                                      "Query",
                                                      "Count",
                                                      "Avg Execution Time(ms)"])
                        try:
                            for query in sorted(ql.query_stats.keys()):
                                if ql.query_stats[query][1] > 0:
                                    self.query_table.add_row([str(ql.bucket.name),
                                                              ql.bucket.query_map[query][0],
                                                              ql.query_stats[query][1],
                                                              ql.query_stats[query][0]/ql.query_stats[query][1]])
                        except Exception as e:
                            print(e)
                        self.query_table.display("N1QL Query Execution Stats")
                        self.table.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                    self.table.display("N1QL Query Statistics")

                    self.FTStable = TableView(self.log.info)
                    self.FTStable.set_headers(["Bucket",
                                               "Total Queries",
                                               "Failed Queries",
                                               "Success Queries",
                                               "Rejected Queries",
                                               "Cancelled Queries",
                                               "Timeout Queries",
                                               "Errored Queries"])
                    for ql in self.ftsQL:
                        self.FTStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                    self.FTStable.display("FTS Query Statistics")

                    self.CBAStable = TableView(self.log.info)
                    self.CBAStable.set_headers(["Bucket",
                                                "Total Queries",
                                                "Failed Queries",
                                                "Success Queries",
                                                "Rejected Queries",
                                                "Cancelled Queries",
                                                "Timeout Queries",
                                                "Errored Queries"])
                    for ql in self.cbasQL:
                        self.CBAStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                    self.CBAStable.display("CBAS Query Statistics")

                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def test_rebalance(self):
        self.monitor_query_status()
        cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                       kwargs={"cluster": self.cluster})
        cpu_monitor.start()
        num_items = self.input.param("num_items", 5000000)

        self.loadDefn1 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 2,
            "num_items": num_items,
            "start": 0,
            "end": num_items,
            "ops": 100000,
            "doc_size": 1024,
            "pattern": [0, 50, 50, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 200,
            "ftsQPS": 10,
            "cbasQPS": 10,
            "collections_defn": [
                {
                    "valType": "Hotel",
                    "2i": [2, 2],
                    "FTS": [0, 0],
                    "cbas": [2, 2, 2]
                },
                {
                    "valType": "Hotel",
                    "2i": [2, 2],
                    "FTS": [2, 2],
                    "cbas": [1, 1, 1]
                }
                ]
            }
        self.nimbus = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 2,
            "num_items": self.input.param("num_items", 1500000000),
            "start": 0,
            "end": self.input.param("num_items", 1500000000),
            "ops": 100000,
            "doc_size": 1024,
            "pattern": [0, 50, 50, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 200,
            "ftsQPS": 0,
            "cbasQPS": 0,
            "collections_defn": [
                {
                    "valType": "NimbusP",
                    "2i": [2, 2],
                    "FTS": [0, 0],
                    "cbas": [0, 0, 0]
                },
                {
                    "valType": "NimbusM",
                    "2i": [2, 2],
                    "FTS": [0, 0],
                    "cbas": [0, 0, 0]
                }
                ]
            }
        self.sanity = {
            "valType": "SimpleValue",
            "scopes": 1,
            "collections": 2,
            "num_items": 50000000,
            "start": 0,
            "end": 50000000,
            "ops": 40000,
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 10,
            "ftsQPS": 10,
            "cbasQPS": 10,
            "collections_defn": [
                {
                    "valType": "SimpleValue",
                    "2i": [5, 5],
                    "FTS": [2, 2],
                    "cbas": [2, 2, 2]
                    },
                {
                    "valType": "SimpleValue",
                    "2i": [5, 5],
                    "FTS": [5, 5],
                    "cbas": [2, 2, 2]
                    }
                ]
            }
        sanity = self.input.param("sanity", False)
        nimbus = self.input.param("nimbus", False)
        if sanity:
            self.load_defn.append(self.sanity)
        elif nimbus:
            self.load_defn.append(self.nimbus)
        else:
            self.load_defn.append(self.loadDefn1)

        #######################################################################
        if not self.skip_init:
            self.create_buckets()
        else:
            for i, bucket in enumerate(self.cluster.buckets):
                bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                num_clients = self.input.param("clients_per_db",
                                               min(5, bucket.loadDefn.get("collections")))
                self.create_sdk_client_pool([bucket],
                                            num_clients)
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if bucket.loadDefn.get("collections") > 0:
                        self.collection_prefix = self.input.param("collection_prefix",
                                                                  "VolumeCollection")

                        for i in range(bucket.loadDefn.get("collections")):
                            collection_name = self.collection_prefix + str(i)
                            collection_spec = {"name": collection_name}
                            CollectionUtils.create_collection_object(bucket, scope, collection_spec)

        self.skip_read_on_error = True
        self.suppress_error_table = True
        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''

        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in self.cluster.buckets:
            self.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=bucket.loadDefn.get("num_items")/2,
                               bucket=bucket)
        if not self.skip_init:
            self.perform_load(validate_data=False, buckets=self.cluster.buckets, overRidePattern=[100,0,0,0,0])
            if self.xdcr_remote_clusters > 0:
                self.drXDCR.set_up_replication()

        self.PrintStep("Step 3: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in self.cluster.buckets:
            self.generate_docs(doc_ops=["create"],
                               create_start=bucket.loadDefn.get("num_items")/2,
                               create_end=bucket.loadDefn.get("num_items"),
                               bucket=bucket)
        if not self.skip_init:
            self.perform_load(validate_data=False, buckets=self.cluster.buckets, overRidePattern=[100,0,0,0,0])

        if self.cluster.cbas_nodes:
            self.drCBAS.create_datasets(self.cluster.buckets)
            self.drCBAS.create_indexes(self.cluster.buckets)
            result = self.drCBAS.wait_for_ingestion(
                self.cluster.buckets, self.index_timeout)
            self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("cbasQPS", 0) > 0:
                    ql = CBASQueryLoad(bucket)
                    ql.start_query_load()
                    self.cbasQL.append(ql)

        if self.cluster.index_nodes:
            self.drIndex.create_indexes(self.cluster.buckets)
            self.drIndex.build_indexes(self.cluster.buckets, wait=True)
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("2iQPS", 0) > 0:
                    ql = QueryLoad(bucket)
                    ql.start_query_load()
                    self.ql.append(ql)

        if self.cluster.eventing_nodes:
            self.drEventing.create_eventing_functions()
            self.drEventing.lifecycle_operation_for_all_functions("deploy", "deployed")

        if self.cluster.fts_nodes:
            self.drFTS.create_fts_indexes(self.cluster.buckets)
            status = self.drFTS.wait_for_fts_index_online(self.cluster.buckets,
                                                          self.index_timeout)
            self.assertTrue(status, "FTS index build failed.")
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("ftsQPS", 0) > 0:
                    ql = FTSQueryLoad(bucket)
                    ql.start_query_load()
                    self.ftsQL.append(ql)

        provider = self.input.param("provider", "aws").lower()
        computeList = GCP.compute
        _type = GCP.StorageType.PD_SSD
        if provider == capella_constants.AWS:
            computeList = AWS.compute
            _type = AWS.StorageType.GP3
        storage_type = self.input.param("type", _type).upper()

        self.mutation_perc = self.input.param("mutation_perc", 100)
        for bucket in self.cluster.buckets:
            bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 5000)
            self.generate_docs(bucket=bucket)
        tasks = self.perform_load(wait_for_load=False)
        if not sanity:
            disk_increment = self.input.param("increment", 5)
            while self.loop <= self.iterations:
                self.loop += 1
                time.sleep(1*60*60)
                if self.rebalance_type == "all" or self.rebalance_type == "disk":
                    # Rebalance 1 - Disk Upgrade
                    initial_services = self.input.param("services", "data")
                    server_group_list = list()
                    self.restart_query_load()
                    for service_group in initial_services.split("-"):
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            self.disk[service] = self.disk[service] + disk_increment
                            if service == "query":
                                self.disk[service_group[1]] = self.disk[service_group[1]] + disk_increment
                        config = {
                            "size": self.num_nodes[service],
                            "services": service_group,
                            "compute": self.compute[service],
                            "storage": {
                                "type": storage_type,
                                "size": self.disk[service],
                                "iops": self.iops[service]
                            }
                        }
                        if self.capella_cluster_config.get("place"):
                            if self.capella_cluster_config["place"]["hosted"]["provider"] != "aws":
                                config["storage"].pop("iops")
                        elif self.capella_cluster_config["provider"] != "hostedAWS":
                            config["storage"].pop("iops")
                        server_group_list.append(config)
                    self.log.info(server_group_list)
                    if self.backup_nodes > 0:
                        self.drBackupRestore.backup_now(wait_for_backup=False)
                    rebalance_task = self.task.async_rebalance_capella(self.cluster,
                                                                       server_group_list,
                                                                       timeout=96*60*60)
                    disk_increment = disk_increment * -1
                    self.sleep(1*60*60)
                    self.restart_query_load()
                    self.task_manager.get_task_result(rebalance_task)
                    self.cluster_util.print_cluster_stats(self.cluster)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                if self.rebalance_type == "all" or self.rebalance_type == "compute":
                    # Rebalance 2 - Compute Upgrade
                    self.restart_query_load()
                    server_group_list = list()
                    initial_services = self.input.param("services", "data")
                    for service_group in initial_services.split("-"):
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if service == "kv" or service == "data":
                            comp = computeList.index(self.compute[service])
                            comp = comp + 1 if len(self.compute) > comp + 1 else comp
                            self.compute[service] = computeList[comp]
                        if "index" in service_group or "query" in service_group:
                            comp = computeList.index(self.compute[service])
                            comp = comp + 1 if len(self.compute) > comp + 1 else comp
                            self.compute[service] = computeList[comp]
                        config = {
                            "size": self.num_nodes[service],
                            "services": service_group,
                            "compute": self.compute[service],
                            "storage": {
                                "type": storage_type,
                                "size": self.disk[service],
                                "iops": self.iops[service]
                            }
                        }
                        if self.capella_cluster_config.get("place"):
                            if self.capella_cluster_config["place"]["hosted"]["provider"] != "aws":
                                config["storage"].pop("iops")
                        elif self.capella_cluster_config["provider"] != "hostedAWS":
                            config["storage"].pop("iops")
                        server_group_list.append(config)
                    self.log.info(server_group_list)
                    if self.backup_nodes > 0:
                        self.drBackupRestore.backup_now(wait_for_backup=False)
                    rebalance_task = self.task.async_rebalance_capella(self.cluster,
                                                                       server_group_list,
                                                                       timeout=96*60*60)
                    self.task_manager.get_task_result(rebalance_task)
                    self.cluster_util.print_cluster_stats(self.cluster)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                if self.rebalance_type == "all" or self.rebalance_type == "disk_compute":
                    # Rebalance 3 - Both Disk/Compute Upgrade
                    self.sleep(1*60*60)
                    self.restart_query_load()
                    server_group_list = list()
                    initial_services = self.input.param("services", "data")
                    for service_group in initial_services.split("-"):
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            self.disk[service] = self.disk[service] + disk_increment
                        if service == "kv" or service == "data":
                            comp = computeList.index(self.compute[service])
                            comp = comp + 1 if len(self.compute) > comp + 1 else comp
                            self.compute[service] = computeList[comp]
                        if "index" in service_group or "gsi" in service_group:
                            comp = computeList.index(self.compute[service])
                            comp = comp + 1 if len(self.compute) > comp + 1 else comp
                            self.compute[service] = computeList[comp]
                        if "query" in service_group or "n1ql" in service_group:
                            comp = computeList.index(self.compute[service])
                            comp = comp + 1 if len(self.compute) > comp + 1 else comp
                            self.compute[service] = computeList[comp]
                        config = {
                            "size": self.num_nodes[service],
                            "services": service_group,
                            "compute": self.compute[service],
                            "storage": {
                                "type": storage_type,
                                "size": self.disk[service],
                                "iops": self.iops[service]
                            }
                        }
                        if self.capella_cluster_config.get("place"):
                            if self.capella_cluster_config["place"]["hosted"]["provider"] != "aws":
                                config["storage"].pop("iops")
                        elif self.capella_cluster_config["provider"] != "hostedAWS":
                            config["storage"].pop("iops")
                        server_group_list.append(config)
                    if self.backup_nodes > 0:
                        self.drBackupRestore.backup_now(wait_for_backup=False)
                    rebalance_task = self.task.async_rebalance_capella(self.cluster,
                                                                       server_group_list,
                                                                       timeout=96*60*60)
                    self.task_manager.get_task_result(rebalance_task)
                    disk_increment = disk_increment * -1
                    self.cluster_util.print_cluster_stats(self.cluster)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.PrintStep("Step 4: XDCR replication being set up")
            if self.xdcr_remote_clusters > 0:
                num_items = self.cluster.buckets[0].loadDefn.get("num_items") * self.cluster.buckets[0].loadDefn.get(
                    "collections")
                replication_done = self.drXDCR.is_replication_complete(
                    cluster=self.xdcr_cluster,
                    bucket_name=self.xdcr_cluster.buckets[0].name,
                    item_count=num_items)
                if not replication_done:
                    self.log.error("Replication did not complete. Check logs!")
            if self.backup_nodes > 0:
                list_backups = self.drBackupRestore.list_all_backups().json()
                backups_on_bucket = list_backups['backups']['data']
                if not backups_on_bucket:
                    self.fail("No backups have been taken on bucket {}".format(self.cluster.buckets[0].name))
                else:
                    for count, item in enumerate(backups_on_bucket):
                        self.log.debug("========= Backup number {} ==========".format(count))
                        self.log.debug("Backup debug info:{}".format(item['data']))
                CapellaAPI.flush_bucket(self.cluster, self.cluster.buckets[0].name)
                time.sleep(120)
                self.drBackupRestore.restore_from_backup(timeout=self.index_timeout)
                time.sleep(60)
                rest = RestConnection(self.cluster.master)
                bucket_info = rest.get_bucket_details(bucket_name=self.cluster.buckets[0].name)
                item_count = self.cluster.buckets[0].loadDefn.get("num_items") * self.cluster.buckets[0].loadDefn.get(
                    "collections")
                if bucket_info['basicStats']['itemCount'] == item_count:
                    self.log.info("Post restore item count on the bucket is {}".format(item_count))
        else:
            self.loop = 0
            self.rebl_nodes = 0
            self.max_rebl_nodes = self.input.param("max_rebl_nodes",
                                                   self.nodes_init + 6)
            while self.loop < self.iterations:
                self.rebl_nodes += 3
                if self.rebl_nodes > self.max_rebl_nodes:
                    self.rebl_nodes = self.nodes_init
                config = self.rebalance_config(self.rebl_nodes)

                ###################################################################
                self.PrintStep("Step 4.{}: Scale UP with Loading of docs".
                               format(self.loop))
                rebalance_task = self.task.async_rebalance_capella(self.cluster,
                                                                   config,
                                                                   timeout=5*60*60)

                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.loop += 1
                self.sleep(60, "Sleep for 60s after rebalance")

            self.loop = 0
            while self.loop < self.iterations:
                self.rebl_nodes -= 3
                self.PrintStep("Step 5.{}: Scale DOWN with Loading of docs".
                               format(self.loop))
                config = self.rebalance_config(self.rebl_nodes)
                rebalance_task = self.task.async_rebalance_capella(self.cluster,
                                                                   config,
                                                                   timeout=5*60*60)

                self.task_manager.get_task_result(rebalance_task)
                self.cluster_util.print_cluster_stats(self.cluster)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(60, "Sleep for 60s after rebalance")

                self.loop += 1
        for task in tasks:
            task.stop_work_load()
        self.wait_for_doc_load_completion(tasks)
        if self.track_failures:
            self.data_validation()
        if self.cluster.eventing_nodes:
            self.drEventing.print_eventing_stats()
        if self.cluster.fts_nodes:
            self.drFTS.discharge_FTS()
        if self.cluster.cbas_nodes:
            self.drCBAS.discharge_CBAS()
        if self.cluster.index_nodes:
            self.drIndex.discharge_N1QL()
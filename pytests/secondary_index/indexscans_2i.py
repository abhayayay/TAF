from Cb_constants import CbServer, DocLoading
from base_2i import BaseSecondaryIndexingTests
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from couchbase_helper.query_definitions import QueryDefinition, \
    FULL_SCAN_TEMPLATE, SIMPLE_INDEX, FULL_SCAN, NO_ORDERBY_GROUPBY
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()
        self.key = "test_query_doc"
        self.sync_write_abort_pattern = \
            self.input.param("sync_write_abort_pattern",
                             "all_aborts")
        self.create_index_during = self.input.param("create_index_during",
                                                    "before_doc_ops")

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def _create_index_in_async(self, query_definitions=None, buckets=None,
                               index_nodes=None):
        refer_index = []
        if buckets is None:
            buckets = self.cluster.buckets
        if query_definitions is None:
            query_definitions = self.query_definitions
        if not self.run_async:
            self.run_multi_operations(buckets=buckets,
                                      query_definitions=query_definitions,
                                      create_index=True)
            return
        if index_nodes is None:
            index_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.INDEX,
                get_all_nodes=True)
        x = len(query_definitions) - 1
        while x > -1:
            tasks = list()
            build_index_map = dict()
            for bucket in buckets:
                build_index_map[bucket.name] = []
            for server in index_nodes:
                for bucket in buckets:
                    if x > -1:
                        key = "{0}:{1}".format(bucket.name,
                                               query_definitions[x].index_name)
                        if key not in refer_index:
                            refer_index.append(key)
                            refer_index.append(query_definitions[x].index_name)
                            deploy_node_info = None
                            if self.use_gsi_for_secondary:
                                deploy_node_info = ["%s:%s" % (server.ip,
                                                               server.port)]
                            build_index_map[bucket.name].append(
                                query_definitions[x].index_name)
                            tasks.append(self.async_create_index(
                                bucket.name,
                                query_definitions[x],
                                deploy_node_info=deploy_node_info))
                x -= 1
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            if self.defer_build:
                for bucket_name in build_index_map.keys():
                    if len(build_index_map[bucket_name]) > 0:
                        build_index_task = self.async_build_index(
                            bucket_name,
                            build_index_map[bucket_name])
                        self.task.jython_task_manager.get_task_result(
                            build_index_task)
                monitor_index_tasks = []
                for bucket_name in build_index_map.keys():
                    for index_name in build_index_map[bucket_name]:
                        monitor_index_tasks.append(
                            self.async_monitor_index(bucket_name, index_name))
                for task in monitor_index_tasks:
                    self.task.jython_task_manager.get_task_result(task)

    def test_multi_create_query_explain_drop_index(self):
        self._create_index_in_async()
        self.run_doc_ops()
        self._query_explain_in_async()
        self._verify_index_map()

        tasks = self.async_run_multi_operations(
            buckets=self.buckets,
            query_definitions=self.query_definitions,
            drop_index=True)
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    # Function to validate index's item count
    def validate_indexed_doc_count(self, bucket, index_item_count):
        self.indexer_rest.wait_for_indexing_to_complete(bucket.name)
        self.sleep(5, "Wait for indexing to complete")
        self.log.info("Validate indexed item count")
        for m_type in index_item_count.keys():
            if m_type == "#primary":
                result = self.n1ql_helper.get_index_count_using_primary_index(
                    self.cluster.buckets)
                if result[bucket.name] != index_item_count[m_type]:
                    self.log_failure("Mismatch in primary num_indexed "
                                     "items: %s, expected: %s"
                                     % (result[bucket.name],
                                        index_item_count[m_type]))
            else:
                result = self.n1ql_helper.run_cbq_query(
                    'SELECT COUNT(*) FROM %s '
                    'USE INDEX (%s) '
                    'WHERE mutation_type="%s"'
                    % (bucket.name, m_type, m_type.split("_")[1].upper()))
                count = int(result['results'][0]['$1'])
                if count != index_item_count[m_type]:
                    self.log_failure("Mismatch in index %s count: %s != %s"
                                     % (m_type, count,
                                        index_item_count[m_type]))

    def validate_indexed_count_from_stats(self, bucket,
                                          expected_num_indexed,
                                          index_item_count):
        gsi_stats = self.indexer_rest.get_index_stats()
        for gsi_index, stats in gsi_stats[bucket.name].items():
            if stats["num_docs_indexed"] != expected_num_indexed[gsi_index]:
                self.log_failure("Bucket::Index - %s:%s:num_docs_indexed "
                                 "%s, expected: %s"
                                 % (bucket.name,
                                    gsi_index,
                                    stats["num_docs_indexed"],
                                    expected_num_indexed[gsi_index]))
            if stats["items_count"] != index_item_count[gsi_index]:
                self.log_failure("Bucket::Index - %s:%s:items_count "
                                 "%s, expected: %s"
                                 % (bucket.name,
                                    gsi_index,
                                    stats["items_count"],
                                    index_item_count[gsi_index]))

    def create_gsi_indexes(self, bucket):
        self.log.info("Create indexes on 'default' bucket")
        query_def = QueryDefinition(
            index_name="durable_add_aborts",
            index_fields=["age", "first_name"],
            query_template=FULL_SCAN_TEMPLATE.format("*",
                                                     "name IS NOT NULL"),
            groups=[SIMPLE_INDEX, FULL_SCAN, "isnotnull",
                    NO_ORDERBY_GROUPBY])
        query = query_def.generate_index_create_query(
            bucket.name,
            use_gsi_for_secondary=True,
            index_where_clause="mutation_type='ADD'")
        self.n1ql_helper.run_cbq_query(query)

        query_def = QueryDefinition(
            index_name="durable_set_aborts",
            index_fields=["age", "first_name"],
            query_template=FULL_SCAN_TEMPLATE.format("*",
                                                     "name IS NOT NULL"),
            groups=[SIMPLE_INDEX, FULL_SCAN, "isnotnull",
                    NO_ORDERBY_GROUPBY])
        query = query_def.generate_index_create_query(
            bucket.name,
            use_gsi_for_secondary=True,
            index_where_clause="mutation_type='SET'")
        self.n1ql_helper.run_cbq_query(query)

    def test_index_with_aborts(self):
        """
        1. Create index (2i/view) on default bucket
        2. Load multiple docs such that all sync_writes will be aborted
        3. Verify nothing went into indexing
        4. Load sync_write docs such that they are successful
        5. Validate the mutated docs are taken into indexing
        :return:
        """

        crud_batch_size = 50
        def_bucket = self.cluster.buckets[0]
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        replica_vbs = dict()
        verification_dict = dict()
        index_item_count = dict()
        expected_num_indexed = dict()
        load_gen = dict()
        load_gen["ADD"] = dict()
        load_gen["SET"] = dict()
        partial_aborts = ["initial_aborts", "aborts_at_end"]

        durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        if self.create_index_during == "before_doc_ops":
            self.create_gsi_indexes(def_bucket)

        curr_items = self.bucket_util.get_bucket_current_item_count(
            self.cluster, def_bucket)
        if self.sync_write_abort_pattern in ["all_aborts", "initial_aborts"]:
            self.bucket_util.flush_bucket(self.cluster, def_bucket)
            self.num_items = 0
        else:
            self.num_items = curr_items

        self.log.info("Disabling auto_failover to avoid node failures")
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Validate vbucket stats
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        # verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        index_item_count["#primary"] = self.num_items
        index_item_count["durable_add_aborts"] = 0
        index_item_count["durable_set_aborts"] = 0
        expected_num_indexed["#primary"] = curr_items
        expected_num_indexed["durable_add_aborts"] = 0
        expected_num_indexed["durable_set_aborts"] = 0

        if self.create_index_during == "before_doc_ops":
            self.validate_indexed_doc_count(def_bucket, index_item_count)

        self.log.info("Loading docs such that all sync_writes will be aborted")
        for server in kv_nodes:
            ssh_shell = RemoteMachineShellConnection(server)
            cbstats = Cbstats(ssh_shell)
            replica_vbs[server] = cbstats.vbucket_list(def_bucket.name,
                                                       "replica")
            load_gen["ADD"][server] = list()
            load_gen["ADD"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="ADD"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["ADD"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="ADD"))
                verification_dict["ops_create"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size
                index_item_count["#primary"] += crud_batch_size
                index_item_count["durable_add_aborts"] += crud_batch_size
                expected_num_indexed["#primary"] += crud_batch_size
                expected_num_indexed["durable_add_aborts"] += crud_batch_size

            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["ADD"][server], self.cluster, def_bucket,
                self.durability_level, DocLoading.Bucket.DocOps.CREATE,
                self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            if self.create_index_during == "before_doc_ops":
                self.validate_indexed_doc_count(def_bucket, index_item_count)

            load_gen["SET"][server] = list()
            load_gen["SET"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="SET"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["SET"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="SET"))
                verification_dict["ops_update"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size
                index_item_count["durable_add_aborts"] -= crud_batch_size
                index_item_count["durable_set_aborts"] += crud_batch_size
                expected_num_indexed["#primary"] += crud_batch_size
                expected_num_indexed["durable_add_aborts"] += crud_batch_size
                expected_num_indexed["durable_set_aborts"] += crud_batch_size

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["SET"][server], self.cluster, def_bucket,
                self.durability_level, DocLoading.Bucket.DocOps.UPDATE,
                self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")

            ssh_shell.disconnect()

            if self.create_index_during == "before_doc_ops":
                self.validate_indexed_doc_count(def_bucket, index_item_count)
        failed = durability_helper.verify_vbucket_details_stats(
            def_bucket, kv_nodes,
            vbuckets=self.cluster.vbuckets, expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details verification failed")
        self.validate_test_failure()

        if self.create_index_during == "after_doc_ops":
            self.create_gsi_indexes(def_bucket)
            self.validate_indexed_doc_count(def_bucket, index_item_count)

        self.log.info("Verify aborts are not indexed")
        self.validate_indexed_count_from_stats(def_bucket,
                                               expected_num_indexed,
                                               index_item_count)

        if not self.use_gsi_for_primary:
            self.log.info("Wait of any indexing_activity to complete")
            index_monitor_task = self.cluster_util.async_monitor_active_task(
                self.cluster.master,
                "indexer",
                "_design/ddl_#primary",
                num_iteration=20,
                wait_task=True)[0]
            self.task_manager.get_task_result(index_monitor_task)
            self.assertTrue(index_monitor_task.result,
                            "Indexer task still running on server")

        for server in kv_nodes:
            if self.sync_write_abort_pattern == "initial_aborts":
                load_gen["ADD"][server] = load_gen["ADD"][server][:1]
                load_gen["SET"][server] = load_gen["SET"][server][:1]
            elif self.sync_write_abort_pattern == "aborts_at_end":
                load_gen["ADD"][server] = load_gen["ADD"][server][-1:]
                load_gen["SET"][server] = load_gen["SET"][server][-1:]

        self.log.info("Load sync_write docs such that they are successful")
        for server in kv_nodes:
            for gen_load in load_gen["ADD"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "create", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)

                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")

                index_item_count["#primary"] += crud_batch_size
                index_item_count["durable_add_aborts"] += crud_batch_size
                expected_num_indexed["#primary"] += crud_batch_size
                expected_num_indexed["durable_add_aborts"] += crud_batch_size
                self.validate_indexed_doc_count(def_bucket, index_item_count)

            for gen_load in load_gen["SET"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "update", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)

                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")

                index_item_count["durable_add_aborts"] -= crud_batch_size
                index_item_count["durable_set_aborts"] += crud_batch_size
                expected_num_indexed["#primary"] += crud_batch_size
                expected_num_indexed["durable_add_aborts"] += crud_batch_size
                expected_num_indexed["durable_set_aborts"] += crud_batch_size
                self.validate_indexed_doc_count(def_bucket,
                                                index_item_count)

        self.log.info("Validate the mutated docs are taken into indexing")
        self.validate_indexed_count_from_stats(def_bucket,
                                               expected_num_indexed,
                                               index_item_count)
        self.validate_test_failure()

from bucket_collections.collections_base import CollectionBase


class BucketParams(CollectionBase):
    def setUp(self):
        super(BucketParams, self).setUp()
        self.bucket = self.cluster.buckets[0]
        # To override default num_items to '0'
        self.num_items = self.input.param("num_items", 10000)
        load_spec = \
            self.input.param("load_spec", "def_load_random_collection")
        self.doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(load_spec)
        self.replica_count = self.input.param("replica_count", 4)
        self.buckets = self.cluster.buckets

    def test_update_replica(self):
        """ load documents, update replica, verify docs"""
        if self.nodes_init < 2:
            self.log.error("Test not supported for < 2 node cluster")
            return

        for new_replica in range(1, min(self.replica_count, self.nodes_init)):
            # Change replica and perform doc loading
            self.log.info("new replica is %s" % new_replica)
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        new_replica)
            self.load_docs(self.task, self.cluster, self.buckets,
                           self.doc_loading_spec)
            self.validate_docs(self.buckets)

        for new_replica in range(min(self.replica_count,
                                     self.nodes_init) - 1, -1, -1):
            self.log.info("new replica is %s" % new_replica)
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        new_replica)
            self.load_docs(self.task, self.cluster, self.buckets,
                           self.doc_loading_spec)
            self.validate_docs(self.buckets)

    def test_update_replica_node(self):
        """ update replica, add/remove node verify docs"""
        count = 0
        for new_replica in range(1, self.replica_count):
            # Change replica and perform doc loading
            self.log.info("Setting replica = %s" % new_replica)
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        new_replica)
            servs_in = [self.cluster.servers[count + self.nodes_init]]
            rebalance_task = self.task.async_rebalance(
                self.cluster, servs_in, [], retry_get_process_num=self.retry_get_process_num)
            self.sleep(10, "wait for rebalance to start")
            self.load_docs(self.task, self.cluster, self.buckets,
                           self.doc_loading_spec)
            self.task_manager.get_task_result(rebalance_task)
            if rebalance_task.result is False:
                self.fail("Rebalance failed with replica: %s" % new_replica)
            count = count + 1
            self.validate_docs(self.buckets)

        for new_replica in range(self.replica_count - 1, 0, -1):
            self.log.info("Setting replica = %s" % new_replica)
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        new_replica)
            servs_out = [self.cluster.nodes_in_cluster[-1]]
            rebalance_task = self.task.async_rebalance(
                self.cluster, [], servs_out, retry_get_process_num=self.retry_get_process_num)
            self.sleep(10, "Wait for rebalance to start")
            self.load_docs(self.task, self.cluster, self.buckets,
                           self.doc_loading_spec)
            self.task_manager.get_task_result(rebalance_task)
            if rebalance_task.result is False:
                self.fail("Rebalance failed with replica: %s" % new_replica)
            self.validate_docs(self.buckets)

    def load_docs(self, task, cluster, buckets, load_spec):
        # Load docs
        doc_loading_task = self.bucket_util.run_scenario_from_spec(task, cluster,
                                                buckets, load_spec, batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Doc_loading failed")

    def validate_docs(self, buckets):
        # Validate doc count as per bucket collections
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.validate_doc_count_as_per_collections(
            self.cluster, buckets[0])
        self.validate_test_failure()

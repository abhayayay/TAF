import time

from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from gsiLib.GsiHelper_Rest import GsiHelper
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from storage.plasma.plasma_base import PlasmaBaseTest

class PlasmaServerless(PlasmaBaseTest, ServerlessOnPremBaseTest):
    def setUp(self):
        super(PlasmaServerless, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"
        self.discretionaryQuotaThreshold = self.input.param("discretionaryQuotaThreshold", 0.0)
        with_default_bucket = self.input.param("with_default_bucket", False)
        self.enableInMemoryCompression = self.input.param("enableInMemoryCompression", True)
        if with_default_bucket:
            old_weight = self.bucket_weight
            self.bucket_weight = 1
            self.create_bucket(self.cluster)
            self.bucket_weight = old_weight
        self.bucket_util.print_bucket_stats(self.cluster)
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        index_node = self.cluster.index_nodes[0]
        if self.in_mem_comp:
            self.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableCompressDuringBurst": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableCompressDuringBurst": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.settings.compaction.plasma.manual": self.manual},
                                    index_node)
            self.set_index_settings({"indexer.plasma.purger.enabled": self.purger_enabled},
                                    index_node)
            self.set_index_settings({"indexer.plasma.serverless.discretionaryQuotaThreshold": self.discretionaryQuotaThreshold},
                                    index_node)
        self.stat_field = 'resident_ratio'
        self.stat_obj_list = self.create_Stats_Obj_list()
        self.indexer_client = RestConnection(self.cluster.index_nodes[0])
        self.index_mem_quota = self.input.param("index_mem_quota", 1024)
        self.indexer_client.set_service_mem_quota(
            {CbServer.Settings.INDEX_MEM_QUOTA: self.index_mem_quota})
        self.append_items = self.input.param("append_items", 40000)
        self.timeout = self.input.param("timeout", 1000)
        self.sync_create_index = self.input.param("sync_create_index", False)
        self.retry = self.input.param("retry", 3)
        self.scan_in_sync = self.input.param("scan_in_sync", True)
        self.scan_item_count = self.input.param("scan_item_count", 20000)

        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                    replica=self.index_replicas,
                                                                                    defer=True,
                                                                                    number_of_indexes_per_coll=self.index_count,
                                                                                    field=field,
                                                                                    sync=self.sync_create_index,
                                                                                    retry=self.retry)
        self.index_map = indexMap
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, indexMap)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(indexMap, timeout=self.timeout),
                        "polling for deferred indexes failed")
        self.wait_for_stats_to_settle_down(self.stat_obj_list, "equal", "lss_fragmentation")
        self.bucket_util.print_bucket_stats(self.cluster)


    def __get_bucket_params(self, b_name, ram_quota=256, width=1, weight=1):
        self.log.debug("Creating bucket param")
        return {
            Bucket.name: b_name,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: ram_quota,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.width: width,
            Bucket.weight: weight
        }


    def print_stats(self, stats_map):
        total_tenants_quota = 0
        for node in stats_map:
            self.log.info(node)
            for buck in stats_map[node]:
                self.log.info(buck.upper())
                total_tenants_quota += stats_map[node][buck]['quota']
                self.log.info("QUOTA {}".format(stats_map[node][buck]['quota']))
                self.log.info("MANDATORY QUOTA {}".format(stats_map[node][buck]['mandatory_quota']))
                self.log.info("RESIDENT QUOTA {}".format(stats_map[node][buck]['resident_quota']))
                self.log.info("DISCRETIONARY QUOTA {}".format(stats_map[node][buck]['discretionary_quota']))
                self.log.info("PRESSURE {}".format(stats_map[node][buck]['quota'] - stats_map[node][buck]['mandatory_quota'] < 0))
                self.log.info("RR {}".format(stats_map[node][buck]['resident_ratio']))

        self.log.info("TOTAL TENANT ACTIVE QUOTA: {}".format(total_tenants_quota)) 


    def create_workload(self, start, bucket_list):
        end = start + self.append_items
        load_gen = doc_generator(self.key, start, end, key_size=self.key_size)
        start = end
        self.load_to_bucket_list(bucket_list, load_gen)
        self.bucket_util.print_bucket_stats(self.cluster)
        return start


    def total_tenants_quota(self, bucket_list=None):
        if bucket_list == None:
            bucket_list = self.cluster.buckets
        total_tenants_quota = 0
        bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=bucket_list)
        for node in bucket_quota_map:
            for buck in bucket_quota_map[node]:
                total_tenants_quota += bucket_quota_map[node][buck]['quota']
        self.log.info("Total tenant active quota: {}".format(total_tenants_quota)) 
        return bucket_quota_map, total_tenants_quota


    def full_scan(self, bucket_list, totalCount, offset, numThreadsPerNode=2, lightLoad=True):
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, self.index_map, key='body',
                                                        bucket_list=bucket_list,
                                                        totalCount=totalCount,
                                                        limit=self.query_limit, is_sync=self.scan_in_sync, 
                                                        offSetBound=offset, numThreadsPerNode=numThreadsPerNode,
                                                        lightLoad=lightLoad)
        for taskInstance in query_tasks_info:
            self.log.debug("Checking for full scan status")
            self.task.jython_task_manager.get_task_result(taskInstance)


    def test_quota_extended_to_under_allocated_tenants(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Continue the creates on half the tenants while keeping other tenants idle for 15 mins
        6. Capture quota stats again. 
        7. Expect increase in quota of busy tenants and decrese in quota of light tenants.
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        # for the next 15 mins, keep the even tenants idle and odd tenants occupied
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))
            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
           
        final_even_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, even_bucket_list)
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
    
        self.log.debug("Expecting dip in quota")
        res1 = self.compare_RR_for_nodes(final_even_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='equalsOrLesserThan')
        self.log.debug("Expecting increase in quota")
        res2 = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='equalsOrGreaterThan')
        self.assertTrue(res1, "Dip failed")
        self.assertTrue(res2, "Raise failed")


    def test_no_impact_between_tenants(self):
        """
        1. Load data into buckets and create indexes
        2. Load data till all indexes are around 10% RR
        3. Perform full scans for half an hour
        4. Ensure that there is not much significant change in quotas
        """
        counter = 1
        start = self.init_items_per_collection
        rr_map = [1]

        def rr_within_range(rr_map, threshold):
            for i in rr_map:
                if i > threshold + 0.03:
                    return False 
            return True

        # Load data till all buckets are within 10%-13% RR
        while not rr_within_range(rr_map, 0.1) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            counter += 1
            del rr_map[:]

            # loads for all buckets
            start = self.create_workload(start, self.cluster.buckets)

            # check rr
            bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)
            for node in bucket_quota_map:
                for buck in bucket_quota_map[node]:
                    rr_map.append(bucket_quota_map[node][buck]['resident_ratio'])

            self.bucket_util.print_bucket_stats(self.cluster)

        # Start scans
        init_bucket_quota_map = {}        
        totalCount = self.scan_item_count
        offset=0
        end_time = time.time() + self.wait_timeout
        half_time = time.time() + (self.wait_timeout // 2)
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        counter = 1
        flag = True
        while time.time() < end_time:
            self.full_scan(self.cluster.buckets, totalCount, offset)
            self.log.debug("Perform scan {} times ".format(counter))
            if time.time() >= half_time and flag:
                init_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)
                flag = False
                self.log.info("Taking initial stats...")
            counter+=1

            self.print_stats(self.create_nodes_tenant_stat_map(self.cluster.index_nodes))

        final_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)

        result = self.compare_RR_for_nodes(final_bucket_quota_map, init_bucket_quota_map, field='quota', comparisonType='percent', threshold=5)
        self.assertTrue(result, "Quotas changing more than 5%")
       

    def test_quota_given_to_active_tenant(self):
        """
        1. Create buckets, load data and create indexes.
        2. Keep loading data for 1 tenant while performing scans on others
        3. Ensure that the quota allotted to the heavy tenant goes up and the total quota allotted goes up
        """
        init_bucket_quota_map = {}
        init_total_quota = 0

        total_tenants_quota = 0
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        totalCount = self.scan_item_count
        offset = 0
        start = self.init_items_per_collection
        heavy_tenant = self.cluster.buckets[0].name
        self.log.info("Heavy tenant:", heavy_tenant)
        counter = 0
        end_time = time.time() + self.wait_timeout
        half_time = time.time() + (self.wait_timeout//2)
        half_time_flag = True
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
    
        while total_tenants_quota < plasma_quota and time.time() < end_time:
            counter += 1
            print(("Loop number ", counter))
            # full scans for all except 1 bucket
            self.full_scan(self.cluster.buckets[1:], totalCount, offset)

            # loads for 1 bucket
            bucket_list = [self.cluster.buckets[0]]
            start = self.create_workload(start, bucket_list)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            self.print_stats(inter_bucket_quota_map)
            self.bucket_util.print_bucket_stats(self.cluster)

            if half_time_flag and time.time() >= half_time:
                init_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=self.cluster.buckets)
                init_total_quota = total_tenants_quota
                self.log.info("Initial map created")
                half_time_flag = False

        if total_tenants_quota >= plasma_quota:
            self.log.error("No unused memory. Re-run test with lesser workload.")
            self.assertTrue(False, "No unused memory. Re-run test with lesser workload.")
        final_bucket_heavy_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=[self.cluster.buckets[0]])

        self.log.info("INITIAL TOTAL TENANTS QUOTA: {}".format(init_total_quota))
        self.log.info("FINAL TOTAL TENANTS QUOTA: {}".format(total_tenants_quota))

        res = self.compare_RR_for_nodes(final_bucket_heavy_quota_map, init_bucket_quota_map, field='quota', comparisonType='normal', ops='greater')
        self.assertTrue(res, "Quota did not increase for tenant 1")
        self.assertTrue(total_tenants_quota >= init_total_quota, "Total quota has not increased")


    def test_quota_increase_on_index_drop(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Drop some indexes from the light tenants
        5. Repeat step 3 for 15 mins
        6. Capture quota stats again. 
        7. Expect increase in quota of heavy tenants.
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection
        # if tenants_equal is true, then all tenants get the same workload
        tenants_equal = self.input.param("tenants_equal", False)

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024

        scan_bucket_list = even_bucket_list
        load_bucket_list = odd_bucket_list
        if tenants_equal:
            scan_bucket_list = load_bucket_list = self.cluster.buckets

        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(scan_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, load_bucket_list)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in load_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        # Drop some indexes from all even buckets
        newIndexMap = {}
        for bucket in even_bucket_list:
            scope_dict = self.index_map[bucket.name]
            if tenants_equal:
                newIndexMap[bucket.name] = scope_dict
            else:
                newIndexMap[bucket.name] = dict(list(scope_dict.items())[len(scope_dict)//2:])
        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, newIndexMap,
                                                                         buckets=even_bucket_list, 
                                                                         drop_only_given_indexes=True)

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # for the next 15 mins, scan even tenants and keep odd tenants occupied
        if tenants_equal:
            scan_bucket_list = load_bucket_list = odd_bucket_list
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))

            # full scans for even buckets
            self.full_scan(scan_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))

            # loads for odd buckets
            start = self.create_workload(start, load_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
            self.bucket_util.print_bucket_stats(self.cluster)
           
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
    
        self.log.debug("Expecting increase in quota")
        res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
        self.assertTrue(res, "Raise failed")


    def test_index_drop_from_heavy_tenant(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Drop some indexes from all heavy tenants
        5. Continue scans on half tenants and creates on other half.
        6. Capture quota stats again. 
        7. Expect increase in quota of busy tenants
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection
        rr_map = [1]

        def rr_within_range(rr_map, threshold):
            for i in rr_map:
                if i > threshold + 0.03:
                    return False 
            return True

        # till unused quota is used up, all odd tenants go under pressure, and are 4~7% RR, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0 or not rr_within_range(rr_map, 0.04)) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            del rr_map[:]
            counter += 1

            # full scans for even buckets
            self.log.debug("Perform full scan {} times ".format(counter))
            self.full_scan(even_bucket_list, totalCount, offset)
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.bucket_util.print_bucket_stats(self.cluster)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])
                    rr_map.append(inter_bucket_quota_map[node][buck.name]['resident_ratio'])
            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        # Drop some indexes from all odd buckets
        newIndexMap = {}
        for bucket in odd_bucket_list:
            scope_dict = self.index_map[bucket.name]
            newIndexMap[bucket.name] = dict(list(scope_dict.items())[len(scope_dict)//2:])
        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, newIndexMap,
                                                                         buckets=odd_bucket_list, 
                                                                         drop_only_given_indexes=True)

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # for the next 15 mins, scan even tenants and keep odd tenants occupied
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
            self.bucket_util.print_bucket_stats(self.cluster)
           
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
    
        self.log.debug("Expecting increase in quota")
        res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
        self.assertTrue(res, "Raise failed")


    def test_quota_increase_on_item_delete(self):
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        _, total_tenants_quota = self.total_tenants_quota()
        start = self.init_items_per_collection

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024

        scan_bucket_list = load_bucket_list = self.cluster.buckets

        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(scan_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
            # loads for odd buckets
            start = self.create_workload(start, load_bucket_list)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in load_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)
            self.print_stats(inter_bucket_quota_map)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test.")
        self.log.info("No unused quota left and odd tenants under pressure.")
        self.log.info("Total quota before deletion {}".format(total_tenants_quota))
        self.print_stats(inter_bucket_quota_map)

        self.delete_start = int(start * 0.5)
        self.delete_end = start - 1
        self.log.debug("self.delete_start is {} self.delete_end is {} ".format(
                self.delete_start, self.delete_end))
        # self.process_concurrency = 1
        self.generate_docs(doc_ops="delete")
        self.gen_create = None
        self.gen_read = None
        task = self.data_load(even_bucket_list, skip_default=True)
        self.wait_for_doc_load_completion(task)
        self.sleep(60, "Check docs")

        # trigger compaction
        try:
            self.perform_plasma_mem_ops("compactAll")
        except Exception as e:
            self.log.info("Manual compaction failed")

        stats, total_tenants_quota = self.total_tenants_quota()
        self.log.info("Total quota after deletion {}".format(total_tenants_quota))
        self.print_stats(stats)

        scan_bucket_list = even_bucket_list
        load_bucket_list = odd_bucket_list
        # for the next 15 mins, scan even tenants and keep odd tenants occupied
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))

            # full scans for even buckets
            self.full_scan(scan_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))

            # loads for odd buckets
            start = self.create_workload(start, load_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
            self.bucket_util.print_bucket_stats(self.cluster)
            self.print_stats(self.create_nodes_tenant_stat_map(self.cluster.index_nodes))
           
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)

        self.log.debug("Expecting increase in quota")
        res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
        self.assertTrue(res, "Raise failed")


    def test_tenant_quota_on_heavy_bucket_drop(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Drop a heavy tenant
        6. Continue scans on half tenants and creates on other half.
        7. Capture quota stats again. 
        8. Expect increase in quota of busy tenants
        9. Repeat 5-8 multiple times
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection
        rr_map = [1]

        def rr_within_range(rr_map, threshold):
            for i in rr_map:
                if i > threshold + 0.03:
                    return False 
            return True

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        init_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0 or not rr_within_range(rr_map, 0.04)) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            del rr_map[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            init_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in init_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(init_bucket_quota_map[node][buck.name]['quota'] - init_bucket_quota_map[node][buck.name]['mandatory_quota'])
                    rr_map.append(init_bucket_quota_map[node][buck.name]['resident_ratio'])
            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        self.print_stats(init_bucket_quota_map)

        heavy_tenants_to_drop = self.filter_buckets(odd_bucket_list, filter='even')
        heavy_tenants_to_retain = self.filter_buckets(odd_bucket_list, filter='odd')

        for bucket in heavy_tenants_to_drop:
            self.log.info("Deleting bucket=={}".format(bucket.name))
            self.assertTrue(self.bucket_util.delete_bucket(self.cluster, bucket), "Not able to delete the bucket")
            self.sleep(30, "waiting for 30 seconds after deletion of bucket")
            odd_bucket_list.remove(bucket)

            total_tenants_quota = 0
            pressure = [1]
            while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
                self.log.info("LOOP NUMBER: {}".format(counter))
                del pressure[:]
                counter += 1

                # full scans for even buckets
                self.full_scan(even_bucket_list, totalCount, offset)
                self.log.debug("Perform full scan {} times ".format(counter))
                
                # loads for odd buckets
                start = self.create_workload(start, odd_bucket_list)

                # calculate total active quota and pressure
                final_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
                for node in final_bucket_quota_map:
                    for buck in odd_bucket_list:
                        pressure.append(final_bucket_quota_map[node][buck.name]['quota'] - final_bucket_quota_map[node][buck.name]['mandatory_quota'])
                self.bucket_util.print_bucket_stats(self.cluster)

            final_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=heavy_tenants_to_retain)
            res = self.compare_RR_for_nodes(final_bucket_quota_map, init_bucket_quota_map, field='quota', comparisonType='normal', ops='greater')
            self.assertTrue(res, "Quota not increasing on heavy tenant drop")
            init_bucket_quota_map = final_bucket_quota_map


    def test_round_robin_quota_extended_to_under_allocated_tenants(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Store quota for each index
        4. Keep some bucket's indexes in idle state meanwhile do a perform scan and mutations in other bucket's indexes
        5. Make sure quota should go down for idle indexes and should go up/keep constant for other bucket's indexes
        """
        
        end_time = time.time() + self.wait_timeout

        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        self.bucket_util.print_bucket_stats(self.cluster)
        totalCount = self.init_items_per_collection
        
        # Scan odd bucket indexes
        start = self.init_items_per_collection
        self.iteration = self.input.param("iteration", 10)
        bucket_list_instance = list()
        totalCount = self.scan_item_count
        for item in range(self.iteration):
            if item % 2 == 0:
                bucket_list_instance = even_bucket_list
            else:
                bucket_list_instance = odd_bucket_list
            counter = 0
            initial_bucket_rr_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)
            initial_memory_used = self.get_index_stat("memory_used")
            initial_quota_used = self.get_plasma_index_stat_value("memory_quota", self.stat_obj_list)

            while time.time() < end_time:
                self.full_scan(bucket_list_instance, totalCount, 0)
                self.log.debug("Perform full scan {} times for range ".format(counter, item))
                start = self.create_workload(start, bucket_list_instance)
                self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
                counter += 1
            end_time = time.time() + self.wait_timeout
            final_memory_used = self.get_index_stat("memory_used")
            final_quota_used = self.get_plasma_index_stat_value("memory_quota", self.stat_obj_list)
            self.log.debug("initial memory used are {} and {}".format(initial_memory_used, initial_quota_used))
            self.log.debug("final memory used are {} and {}".format(final_memory_used, final_quota_used))
            self.log.debug("Even bucket RR")
            final_even_bucket_stat_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, even_bucket_list)
            self.log.debug("Odd bucket RR")
            final_odd_bucket_stat_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
            if item % 2 == 0:
                self.log.debug("even iteration")
                self.compare_RR_for_nodes(final_even_bucket_stat_map, initial_bucket_rr_map, comparisonType='normal', ops='equalsOrGreaterThan', field='quota')
                self.compare_RR_for_nodes(final_odd_bucket_stat_map, initial_bucket_rr_map, comparisonType='normal', ops='equalsOrLesserThan', field='quota')
            else:
                self.log.debug("odd iteration")
                self.compare_RR_for_nodes(final_even_bucket_stat_map, initial_bucket_rr_map, comparisonType='normal', ops='equalsOrLesserThan', field='quota')
                self.compare_RR_for_nodes(final_odd_bucket_stat_map, initial_bucket_rr_map, comparisonType='normal', ops='equalsOrGreaterThan', field='quota')


    def test_change_indexer_quota(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Create pressure on half the tenants while keeping the others active
        4. Capture quota stats
        5. Reduce or increase the indexer quota
        5. Continue scans on half tenants and creates on other half.
        6. Capture quota stats again. 
        7. Expect decrease/increase in quota of busy tenants
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection
        change = self.input.param("change", 'decrease')
        threshold = self.input.param("threshold", 0.89)
        if threshold < 0.89:
            self.sleep(300, "Waiting for total quota to settle")


        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        init_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = threshold * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            init_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            if threshold == 0.89:
                for node in init_bucket_quota_map:
                    for buck in odd_bucket_list:
                        pressure.append(init_bucket_quota_map[node][buck.name]['quota'] - init_bucket_quota_map[node][buck.name]['mandatory_quota'])
            else:
                pressure.append(-1)

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        
        if threshold < 0.89:
            expected_upper_limit = plasma_quota*0.89/threshold if threshold > 0.45 else plasma_quota*0.45/threshold
            self.log.info("Current total: {}, Expected total: {} <= total <= {}".format(total_tenants_quota, plasma_quota, expected_upper_limit))
            self.assertTrue(total_tenants_quota < expected_upper_limit, "No unused quota left")

        self.log.info("Expected quota achieved")

        # change the indexer quota
        if change == 'increase':
            self.indexer_client.set_service_mem_quota(
                {CbServer.Settings.INDEX_MEM_QUOTA: self.index_mem_quota * 2})
        else:
            self.assertTrue(self.index_mem_quota >= 512, "Initial indexer quota needs to be a minimum of 512MB")
            self.indexer_client.set_service_mem_quota(
                {CbServer.Settings.INDEX_MEM_QUOTA: self.index_mem_quota // 2})    
        
        end_time = time.time() + self.wait_timeout
        counter = 1
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.bucket_util.print_bucket_stats(self.cluster)

        final_odd_bucket_quota_map, final_total_tenants_quota = self.total_tenants_quota(bucket_list=odd_bucket_list)

        if change == 'increase':
            self.assertTrue(final_total_tenants_quota > total_tenants_quota, "Total quota has not increased")
            res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, init_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
            self.assertTrue(res, "Quota did not increase for heavy tenants")
        else:
            if threshold > 0.45:
                if threshold == 0.89:
                    self.assertTrue(final_total_tenants_quota < total_tenants_quota, "Total quota has not decreased")
                res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, init_bucket_quota_map, comparisonType='normal', field='quota', ops='lesser')
                self.assertTrue(res, "Quota did not decrease for heavy tenants")
            else:
                res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, init_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
                self.assertTrue(res, "Quota did not increase for heavy tenants")

  
    def test_tenant_quota_on_light_bucket_drop(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Drop a heavy tenant
        6. Continue scans on half tenants and creates on other half.
        7. Capture quota stats again. 
        8. Expect increase in quota of busy tenants
        9. Repeat 5-8 multiple times
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection
        rr_map = [1]

        def rr_within_range(rr_map, threshold):
            for i in rr_map:
                if i > threshold + 0.03:
                    return False 
            return True

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        init_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0 or not rr_within_range(rr_map, 0.04)) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            del rr_map[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            init_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in init_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(init_bucket_quota_map[node][buck.name]['quota'] - init_bucket_quota_map[node][buck.name]['mandatory_quota'])
                    rr_map.append(init_bucket_quota_map[node][buck.name]['resident_ratio'])
            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        self.print_stats(init_bucket_quota_map)

        light_tenants_to_drop = self.filter_buckets(even_bucket_list, filter='even')

        for bucket in light_tenants_to_drop:
            self.log.info("Deleting bucket=={}".format(bucket.name))
            self.assertTrue(self.bucket_util.delete_bucket(self.cluster, bucket), "Not able to delete the bucket")
            self.sleep(30, "waiting for 30 seconds after deletion of bucket")
            even_bucket_list.remove(bucket)

        end_time = time.time() + self.wait_timeout
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota
            final_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            self.bucket_util.print_bucket_stats(self.cluster)

        final_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=odd_bucket_list)
        res = self.compare_RR_for_nodes(final_bucket_quota_map, init_bucket_quota_map, field='quota', comparisonType='normal', ops='greater')
        self.assertTrue(res, "Quota not increasing for heavy tenants on light tenant drop")


    def test_burst_eviction(self):
        indexer_rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection

        # till unused quota is used up and all odd tenants go under pressure
        # keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        init_storage_stats = indexer_rest.get_plasma_stats(nodes_list=self.cluster.index_nodes)

        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))
            self.full_scan(even_bucket_list, totalCount, offset)            
            start = self.create_workload(start, odd_bucket_list)
            counter+=1
            self.bucket_util.print_bucket_stats(self.cluster)
           
        final_storage_stats = indexer_rest.get_plasma_stats(nodes_list=self.cluster.index_nodes)

        for bucket in self.cluster.buckets:
            for scope_name, scope in list(bucket.scopes.items()):
                if scope_name == '_default' or scope_name == '_system':
                    continue
                for _, collection in list(scope.collections.items()):
                    gsi_index_names = self.index_map[bucket.name][scope.name][collection.name]
                    for gsi_index_name in list(gsi_index_names):
                        buck_ind = bucket.name + ':' + scope_name + ':' + collection.name + ':' + gsi_index_name
                        self.assertTrue(
                            final_storage_stats[buck_ind + '_num_burst_visits'] > init_storage_stats[buck_ind + '_num_burst_visits'],
                            "Burst eviction has not occured")
                        
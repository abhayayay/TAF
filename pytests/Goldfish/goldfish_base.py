"""
Created on 17-Oct-2023
@author: Umang Agrawal
"""

from basetestcase import BaseTestCase
from TestInput import TestInputSingleton, TestInputServer
from cbas_utils.cbas_utils import CbasUtil
from goldfishAPI.GoldfishAPIs.DocloadingAPIs.DocloadingAPIs import DocloadingAPIs


class GoldFishBaseTest(BaseTestCase):

    def setUp(self):
        """
        Since BaseTestCase will initialize at least one cluster, we pass service
        for the master node of that cluster
        """
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input

        super(GoldFishBaseTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        if self.use_sdk_for_cbas:
            for cluster in self.user.project.clusters:
                for db_user in cluster.db_users:
                    self.init_sdk_pool_object(
                        cluster, self.sdk_clients_per_user,
                        db_user.username, db_user.password)

        # This is to support static remote clusters. Multiple remote cluster
        # IPs can be passed in format ip1:ip2
        """remote_cluster_ips = self.input.param("remote_cluster_ips", None)
        if remote_cluster_ips:
            remote_cluster_ips = remote_cluster_ips.split("|")
            self.remote_clusters = list()
            for remote_ip in remote_cluster_ips:
                remote_server = copy.deepcopy(self.servers[0])
                remote_server.ip = remote_cluster_ips[i - 1]
                cluster = CBCluster(
                    name=cluster_name, servers=[remote_server])"""

        # Common properties
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.retry_time = self.input.param("retry_time", 300)
        self.num_retries = self.input.param("num_retries", 1)

        self.gf_spec_name = self.input.param("gf_spec_name", None)

        self.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)

        self.doc_loading_server_ip = self.input.param(
            "doc_loading_server_ip", None)
        self.doc_loading_server_port = self.input.param(
            "doc_loading_server_port", None)
        self.doc_loading_APIs = DocloadingAPIs(
            self.doc_loading_server_ip, self.doc_loading_server_port) if (
            self.doc_loading_server_ip) else None

        self.perform_gf_instance_cleanup = self.input.param(
            "perform_gf_instance_cleanup", True)

        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if self.perform_gf_instance_cleanup:
            for cluster in self.user.project.clusters:
                self.cbas_util.cleanup_cbas(cluster)

        super(GoldFishBaseTest, self).tearDown()

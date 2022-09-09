"""
Created on Feb 16, 2022

@author: ritesh.agarwal
"""
from Cb_constants import CbServer
from Jython_tasks.task import DeployDataplane
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.common_utils import Pod, Tenant
from capella_utils.dedicated import CapellaUtils as DedicatedUtils
from capella_utils.serverless import CapellaUtils as ServerlessUtils
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from constants.cloud_constants.capella_constants import AWS
from security_config import trust_all_certs
import global_vars


class OnCloudBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnCloudBaseTest, self).setUp()

        for server in self.input.servers:
            server.type = "serverless"
        # End of framework parameters

        # Cluster level info settings
        self.servers = list()
        self.capella = self.input.capella
        self.num_dataplanes = self.input.param("num_dataplanes", 1)

        self.wait_timeout = self.input.param("wait_timeout", 120)
        CbServer.use_https = True
        trust_all_certs()

        # initialize pod object
        url = self.input.capella.get("pod")
        self.pod = Pod("https://%s" % url,
                       self.input.capella.get("token",
                                              None))

        self.tenant = Tenant(self.input.capella.get("tenant_id"),
                             self.input.capella.get("capella_user"),
                             self.input.capella.get("capella_pwd"))

        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

        self.log_setup_status(self.__class__.__name__, "started")
        self.cluster_name_format = "C%s"
        self.nebula_details = dict()

        self.tenant.project_id = \
            TestInputSingleton.input.capella.get("project", None)
        if not self.tenant.project_id:
            DedicatedUtils.create_project(self.pod, self.tenant, "a_taf_run")

        # Comma separated cluster_ids [Eg: 123-456-789,111-222-333,..]
        self.cb_image = self.input.capella.get("cb_image", "")
        self.dapi_image = self.input.capella.get("dapi_image", "")
        self.dn_image = self.input.capella.get("dn_image", "")
        self.dataplane_id = self.input.capella.get("dataplane", "")
        num_dataplanes = self.input.param("num_dataplanes", 0)
        self.cluster = CBCluster(username=self.rest_username,
                                 password=self.rest_password,
                                 servers=[None] * 40)
        self.cluster.pod = self.pod
        self.cluster.tenant = self.tenant
        self.cluster.type = "serverless"

        tasks = list()
        self.dataplanes = list()
        for _ in range(num_dataplanes):
            self.generate_dataplane_config()
            self.log.info(self.dataplane_config)
            deploy_task = DeployDataplane(self.cluster,
                                          self.dataplane_config,
                                          timeout=self.wait_timeout)
            self.task_manager.add_new_task(deploy_task)
            tasks.append(deploy_task)
        for deploy_task in tasks:
            self.task_manager.get_task_result(deploy_task)
            self.assertTrue(deploy_task.result, "Dataplane deployment failed!")
            self.dataplanes.append(deploy_task.dataplane_id)

        if self.dataplanes:
            self.dataplane_id = self.dataplanes[0]

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        self.serverless_util = ServerlessUtils(self.cluster)
        global_vars.serverless_util = self.serverless_util

    def tearDown(self):
        self.shutdown_task_manager()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()

        if self.skip_teardown_cleanup:
            return
        for bucket in self.cluster.buckets:
            self.serverless_util.delete_database(self.pod, self.tenant, bucket.name)

        for dataplane_id in self.dataplanes:
            self.log.info("Destroying dataplane: {}".format(dataplane_id))
            self.serverless_util.delete_dataplane(dataplane_id)
        if not TestInputSingleton.input.capella.get("project", None):
            DedicatedUtils.delete_project(self.pod, self.tenant)

    def generate_dataplane_config(self):
        provider = self.input.param("provider", AWS.__str__).lower()
        region = self.input.param("region", AWS.Region.US_EAST_1)
        self.dataplane_config = {
            "provider": provider,
            "region": region,
            "overRide": {
                "couchbase": {
                    "image": self.cb_image,
                    "version": "7.2.0",
                }
            }
        }
        if self.dn_image:
            self.dataplane_config["overRide"].update(
                {
                    "nebula": {
                        "image": self.dn_image
                        }
                    }
                )
        if self.dapi_image:
            self.dataplane_config["overRide"].update(
                {
                    "dataApi": {
                        "image": self.dapi_image
                        }
                    }
                )


class ClusterSetup(OnCloudBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()

import re
from Cb_constants import CbServer
from basetestcase import BaseTestCase
import Jython_tasks.task as jython_tasks
from collections_helper.collections_spec_constants import MetaConstants, MetaCrudParams
from couchbase_cli import CouchbaseCLI
from couchbase_helper.documentgenerator import doc_generator
import testconstants
from pytests.ns_server.enforce_tls import EnforceTls
from builds.build_query import BuildQuery
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from scripts.old_install import InstallerJob
from testconstants import CB_REPO, COUCHBASE_VERSIONS, CB_VERSION_NAME, \
    COUCHBASE_MP_VERSION, MV_LATESTBUILD_REPO
from bucket_collections.collections_base import CollectionBase
from couchbase_helper.durability_helper import BucketDurability
from BucketLib.bucket import Bucket


class UpgradeBase(BaseTestCase):
    def setUp(self):
        super(UpgradeBase, self).setUp()
        self.log.info("=== UpgradeBase setUp started ===")
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.0-1776", "3.1.1", "3.1.1-1807",
                                  "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823",
                                  "4.0.0", "4.0.0-4051", "4.1.0", "4.1.0-5005",
                                  "4.5.0", "4.5.0-2601", "4.5.1", "4.5.1-2817",
                                  "4.6.0", "4.6.0-3573", '4.6.2', "4.6.2-3905"]

        self.creds = self.input.membase_settings
        self.key = "test_collections"
        self.initial_version = self.input.param("initial_version",
                                                "6.0.1-2037")
        self.disk_location_data = self.input.param("data_location",
                                                   testconstants.COUCHBASE_DATA_PATH)
        self.disk_location_index = self.input.param(
            "index_location", testconstants.COUCHBASE_DATA_PATH)
        self.upgrade_version = self.input.param("upgrade_version",
                                                "6.5.0-3939")
        self.test_storage_upgrade = \
            self.input.param("test_storage_upgrade", False)
        self.upgrade_type = self.input.param("upgrade_type", "online_swap")
        self.prefer_master = self.input.param("prefer_master", False)
        self.update_nodes = self.input.param("update_nodes", "kv").split(";")
        self.is_downgrade = self.input.param('downgrade', False)
        self.enable_tls = self.input.param('enable_tls', False)
        self.tls_level = self.input.param('tls_level', "all")
        self.upgrade_with_data_load = \
            self.input.param("upgrade_with_data_load", True)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.sync_write_abort_pattern = \
            self.input.param("sync_write_abort_pattern", "all_aborts")

        #### Spec File Parameters ####
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.default")
        self.initial_data_spec = self.input.param("initial_data_spec",
                                                  "initial_load")
        self.sub_data_spec = self.input.param("sub_data_spec",
                                              "subsequent_load_magma")
        self.upsert_data_spec = self.input.param("upsert_data_spec",
                                                 "upsert_load")
        self.sync_write_spec = self.input.param("sync_write_spec",
                                                "sync_write_magma")
        self.collection_spec = self.input.param("collection_spec",
                                                "collections_magma")
        self.load_large_docs = self.input.param("load_large_docs", True)
        self.collection_operations = self.input.param("collection_operations",
                                                      True)
        self.rebalance_op = self.input.param("rebalance_op", "None")
        self.dur_level = self.input.param("dur_level", "default")
        self.alternate_load = self.input.param("alternate_load", False)
        self.complete_cluster_swap = self.input.param("complete_cluster_swap", False)

        # Works only for versions > 1.7 release
        self.product = "couchbase-server"

        if self.initial_version == "same_version":
            self.initial_version = self.upgrade_version

        t_version = float(self.initial_version[:3])
        self.cluster_supports_sync_write = (t_version >= 6.5)
        self.cluster_supports_collections = (t_version >= 7.0)
        self.cluster_supports_system_event_logs = (t_version >= 7.1)
        self.cluster_supports_collections = (t_version >= 7.0)

        self.installer_job = InstallerJob()

        # Dict to map upgrade_type to action functions
        self.upgrade_function = dict()
        self.upgrade_function["online_swap"] = self.online_swap
        self.upgrade_function["online_incremental"] = self.online_incremental
        self.upgrade_function["online_rebalance_in_out"] = \
            self.online_rebalance_in_out
        self.upgrade_function["online_rebalance_out_in"] = \
            self.online_rebalance_out_in
        self.upgrade_function["failover_delta_recovery"] = \
            self.failover_delta_recovery
        self.upgrade_function["failover_full_recovery"] = \
            self.failover_full_recovery
        self.upgrade_function["offline"] = self.offline
        self.upgrade_function["full_offline"] = self.full_offline

        self.__validate_upgrade_type()

        self.log.info("Installing initial version %s on servers"
                      % self.initial_version)
        self.install_version_on_node(
            self.cluster.servers[0:self.nodes_init],
            self.initial_version)

        master_node = self.cluster.servers[0]
        master_rest = RestConnection(master_node)
        if self.disk_location_data == testconstants.COUCHBASE_DATA_PATH and \
                self.disk_location_index == testconstants.COUCHBASE_DATA_PATH:
            # Get service list to initialize the cluster
            if self.services_init:
                self.services_init = self.cluster_util.get_services(
                    [self.cluster.master], self.services_init, 0)

            # Initialize first node in cluster
            if self.services_init:
                master_node.services = self.services_init[0]
            master_rest.init_node()

        # Initialize cluster using given nodes
        for index, server \
                in enumerate(self.cluster.servers[1:self.nodes_init]):
            node_service = None
            if self.services_init and len(self.services_init) > index:
                node_service = self.services_init[index + 1].split(',')
            master_rest.add_node(
                user=server.rest_username, password=server.rest_password,
                remoteIp=server.ip, port=server.port, services=node_service)

        self.task.rebalance(self.cluster.servers[0:self.nodes_init], [], [])
        self.cluster.nodes_in_cluster.extend(
            self.cluster.servers[0:self.nodes_init])
        self.cluster_util.print_cluster_stats(self.cluster)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        self.spec_bucket = self.bucket_util.get_bucket_template_from_package(
            self.spec_name)
        if (self.spec_bucket[Bucket.storageBackend]
                == Bucket.StorageBackend.magma):
            RestConnection(self.cluster.master).set_internalSetting(
                "magmaMinMemoryQuota", 256)

        # Creating buckets from spec file
        CollectionBase.deploy_buckets_from_spec_file(self)

        if "buckets" not in self.spec_bucket:
            self.items_per_col = self.spec_bucket[MetaConstants.NUM_ITEMS_PER_COLLECTION]
        else:
            self.items_per_col = self.spec_bucket["buckets"]["bucket-0"][
                                                MetaConstants.NUM_ITEMS_PER_COLLECTION]

        # Adding RBAC user
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.bucket = self.cluster.buckets[0]

        if self.test_storage_upgrade:
            for i in range(3):
                bucket_name = "testBucket" + str(i)
                self.bucket_util.create_default_bucket(
                    self.cluster,
                    replica=self.num_replicas,
                    compression_mode=self.compression_mode,
                    ram_quota=self.bucket_size,
                    bucket_type=self.bucket_type,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_durability=self.bucket_durability_level,
                    bucket_name=bucket_name)
        if self.enable_tls:
            self.enable_verify_tls(self.cluster.master)
            if self.tls_level == "strict":
                for node in self.cluster.servers:
                    # node.memcached_port = CbServer.ssl_memcached_port (MB-47567)
                    node.port = CbServer.ssl_port

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        if self.dur_level == "majority":
            for bucket in self.cluster.buckets:
                if bucket.name == "bucket-1":
                    self.bucket_util.update_bucket_property(
                        self.cluster.master,
                        bucket,
                        bucket_durability=BucketDurability[
                            Bucket.DurabilityLevel.MAJORITY])

        # Load initial async_write docs into the cluster
        self.log.info("Initial doc generation process starting...")
        CollectionBase.load_data_from_spec_file(self, self.initial_data_spec,
                                                validate_docs=True)
        self.log.info("Initial doc generation completed")

        # Verify initial doc load count
        if self.cluster_supports_collections:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
        else:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)
        self.sleep(30, "Wait for num_items to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        self.spare_node = self.cluster.servers[self.nodes_init]
        if self.complete_cluster_swap:
            self.spare_nodes = self.cluster.servers[self.nodes_init+1:]

        self.gen_load = doc_generator(self.key, 0, self.num_items,
                                      randomize_doc_size=True,
                                      randomize_value=True,
                                      randomize=True)

    def tearDown(self):
        super(UpgradeBase, self).tearDown()

    def _initialize_node_with_new_data_location(self, server, data_location,
                                                index_location,
                                                services=None):
        init_port = server.port or '8091'
        init_tasks = []
        cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
        output, error, _ = cli.node_init(data_location, index_location, None)
        self.log.info(output)
        if error or "ERROR" in output:
            self.log.info(error)
            self.fail("Failed to set new data location. Check error message.")
        init_tasks.append(self.task.async_init_node(
            server, self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled, self.maxParallelIndexers,
            self.maxParallelReplicaIndexers, init_port, self.quota_percent,
            services=services, gsi_type=self.gsi_type))
        for task in init_tasks:
            result = self.task.jython_task_manager.get_task_result(task)
            self.assertTrue((result is None),
                            "nodes initialisation with custom path failed")

    def enable_verify_tls(self, master_node, level=None):
        if not level:
            level = self.tls_level
        rest = RestConnection(master_node)
        node_info = rest.get_nodes_self(10)
        if node_info.version[:5] < '7.0.0' and self.tls_level == "strict":
            rest.enable_encryption()
            task = jython_tasks.FunctionCallTask(
                self.node_utils._enable_tls, [master_node, "control"])
            self.task_manager.schedule(task)
            self.task_manager.get_task_result(task)
            rest.set_encryption_level(level)
        else:
            task = jython_tasks.FunctionCallTask(
                self.node_utils._enable_tls, [master_node, level])
            self.task_manager.schedule(task)
            self.task_manager.get_task_result(task)

        self.assertTrue(EnforceTls.get_encryption_level_on_node(
            master_node) == level)
        if level == "strict":
            status = self.cluster_util.check_if_services_obey_tls(
                self.cluster.nodes_in_cluster)
            self.assertTrue(status, "Services did not honor enforce tls")
            CbServer.use_https = True
            CbServer.n2n_encryption = True

    def __validate_upgrade_type(self):
        """
        Validates input param 'upgrade_type' and
        fails the test in-case of unsupported type.
        :return:
        """
        if self.upgrade_type not in self.upgrade_function.keys():
            self.fail("Unsupported upgrade_type: %s" % self.upgrade_type)

    def fetch_node_to_upgrade(self):
        """
        :return cluster_node: TestServer node to be upgraded.
                              If 'None', no more nodes requires upgrade.
        """

        def check_node_runs_service(node_services):
            for target_service in self.update_nodes:
                if target_service in node_services:
                    return True
            return False

        cluster_node = None
        self.cluster_util.find_orchestrator(self.cluster)

        if self.prefer_master:
            node_info = RestConnection(self.cluster.master).get_nodes_self(10)
            if self.upgrade_version not in node_info.version \
                    and check_node_runs_service(node_info["services"]):
                cluster_node = self.cluster.master

        if cluster_node is None:
            for node in self.cluster_util.get_nodes(self.cluster.master):
                node_info = RestConnection(node).get_nodes_self(10)
                if self.upgrade_version not in node_info.version \
                        and check_node_runs_service(node_info.services):
                    cluster_node = node
                    break

        # Fetch TestServer object from 'Node' object
        if cluster_node is not None:
            cluster_node = self.__getTestServerObj(cluster_node)

        return cluster_node

    def install_version_on_node(self, nodes, version):
        """
        Installs required Couchbase-server version on the target nodes.

        :param nodes: List of nodes to install the cb 'version'
        :param version: Version to install on target 'nodes'
        :return:
        """
        install_params = dict()
        install_params['num_nodes'] = len(nodes)
        install_params['product'] = "cb"
        install_params['version'] = version
        install_params['vbuckets'] = [self.cluster.vbuckets]
        install_params['init_nodes'] = False
        install_params['debug_logs'] = False
        self.installer_job.parallel_install(nodes, install_params)
        if self.disk_location_data != testconstants.COUCHBASE_DATA_PATH or \
                self.disk_location_index != testconstants.COUCHBASE_DATA_PATH:
            master_services = self.cluster_util.get_services(
                self.cluster.servers[:1], self.services_init, start_node=0)
            for node in nodes:
                self._initialize_node_with_new_data_location(
                    node, self.disk_location_data, self.disk_location_index,
                    master_services)

    def __getTestServerObj(self, node_obj):
        for node in self.cluster.servers:
            if node.ip == node_obj.ip:
                return node

    @staticmethod
    def __get_otp_node(rest, target_node):
        """
        Get the OtpNode for the 'target_node'

        :param rest: RestConnection object
        :param target_node: Node going to be upgraded
        :return: OtpNode object of the target_node
        """
        nodes = rest.node_statuses()
        for node in nodes:
            if node.ip == target_node.ip:
                return node

    def __get_rest_node(self, node_to_upgrade):
        """
        Fetch node not going to be involved in upgrade

        :param node_to_upgrade: Node going to be upgraded
        :return: RestConnection object of node
        """
        target_node = None
        for node in self.cluster_util.get_nodes(self.cluster.master):
            if node.ip != node_to_upgrade.ip:
                target_node = node
                break

        return RestConnection(self.__getTestServerObj(target_node))

    def __get_build(self, version, remote, is_amazon=False, info=None):
        if info is None:
            info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in COUCHBASE_VERSIONS:
            if version[:3] in CB_VERSION_NAME:
                build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
            elif version[:5] in COUCHBASE_MP_VERSION:
                build_repo = MV_LATESTBUILD_REPO
        builds, changes = BuildQuery().get_all_builds(
            version=version,
            timeout=self.wait_timeout * 5,
            deliverable_type=info.deliverable_type,
            architecture_type=info.architecture_type,
            edition_type="couchbase-server-enterprise",
            repo=build_repo,
            distribution_version=info.distribution_version.lower())

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        if version[:5] in self.released_versions:
            appropriate_build = BuildQuery(). \
                find_couchbase_release_build(
                '%s-enterprise' % self.product,
                info.deliverable_type,
                info.architecture_type,
                version.strip(),
                is_amazon=is_amazon,
                os_version=info.distribution_version)
        else:
            appropriate_build = BuildQuery(). \
                find_build(builds,
                           '%s-enterprise' % self.product,
                           info.deliverable_type,
                           info.architecture_type,
                           version.strip())

        if appropriate_build is None:
            self.log.info("Builds are: %s \n. Remote is %s, %s. Result is: %s"
                          % (builds, remote.ip, remote.username, version))
            raise Exception("Build %s not found" % version)
        return appropriate_build

    def failover_recovery(self, node_to_upgrade, recovery_type, graceful=True):
        rest = self.__get_rest_node(node_to_upgrade)
        otp_node = self.__get_otp_node(rest, node_to_upgrade)
        self.log.info("Failing over the node {}".format(otp_node.id))
        success = rest.fail_over(otp_node.id,
                                 graceful=graceful)
        if not success:
            self.log_failure("Failover unsuccessful")
            return

        self.cluster_util.print_cluster_stats(self.cluster)

        # Monitor failover rebalance
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.__get_build(self.upgrade_version,
                                             shell)
        self.assertTrue(appropriate_build.url,
                        msg="Unable to find build {}".format(
                            self.upgrade_version))
        self.assertTrue(shell.download_build(appropriate_build),
                        "Failed while downloading the build!")

        self.log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(appropriate_build,
                                                  save_upgrade_config=False,
                                                  forcefully=self.is_downgrade)
        shell.disconnect()
        if not upgrade_success:
            self.log_failure("Upgrade failed")
            return

        rest.set_recovery_type(otp_node.id,
                               recoveryType=recovery_type)

        delta_recovery_buckets = list()
        if recovery_type == "delta":
            delta_recovery_buckets = [bucket.name for bucket in
                                      self.cluster.buckets]

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       deltaRecoveryBuckets=delta_recovery_buckets)
        if self.cluster_supports_collections:
                spec_collection = self.bucket_util.get_crud_template_from_package(
                    self.collection_spec)
                CollectionBase.over_ride_doc_loading_template_params(self, spec_collection)
                CollectionBase.set_retry_exceptions_for_initial_data_load(self, spec_collection)

                spec_collection["doc_crud"][
                    MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

                if self.alternate_load is True:
                    spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                    spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                    spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0

                self.log.info("Performing collection ops during failover rebalance...")
                for iterations in range(5):
                    collection_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        self.buckets_to_load,
                        spec_collection,
                        mutation_num=0,
                        batch_size=500,
                        process_concurrency=4)

                    if collection_task.result is True:
                        self.log.info("Iteration {0} of collection ops done".format(iterations+1))
                        spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                        spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                        spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0
                        self.sleep(5, "Wait for 5 seconds before starting with the next iteration")
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

    def online_swap(self, node_to_upgrade, version,
                    install_on_spare_node=True):
        vb_details = dict()
        vb_verification = dict()
        vb_types = ["active", "replica"]

        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        node_to_upgrade.port = CbServer.port
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]
        if self.enable_tls and self.tls_level == "strict":
            node_to_upgrade.port = CbServer.ssl_port

        # Record vbuckets in swap_node
        if CbServer.Services.KV in services_on_target_node:
            shell = RemoteMachineShellConnection(node_to_upgrade)
            cbstats = Cbstats(shell)
            for vb_type in vb_types:
                vb_details[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            shell.disconnect()
        if install_on_spare_node:
            # Install target version on spare node
            self.install_version_on_node([self.spare_node], version)

        # Perform swap rebalance for node_to_upgrade <-> spare_node
        self.log.info("Swap Rebalance starting...")
        rebalance_passed = self.task.async_rebalance(
            self.cluster_util.get_nodes(self.cluster.master),
            to_add=[self.spare_node],
            to_remove=[node_to_upgrade],
            check_vbucket_shuffling=False,
            services=[",".join(services_on_target_node)],
        )

        if self.upgrade_with_data_load:
            sub_load_spec = self.bucket_util.get_crud_template_from_package(
                self.sub_data_spec)
            CollectionBase.over_ride_doc_loading_template_params(self,
                                                                 sub_load_spec)
            CollectionBase.set_retry_exceptions_for_initial_data_load(self,
                                                                      sub_load_spec)

            sub_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 0
            sub_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10

            update_task_swap = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.buckets_to_load,
                sub_load_spec,
                mutation_num=0,
                batch_size=500,
                process_concurrency=4)

            if self.cluster_supports_collections:
                spec_collection = self.bucket_util.get_crud_template_from_package(
                    self.collection_spec)
                CollectionBase.over_ride_doc_loading_template_params(self, spec_collection)
                CollectionBase.set_retry_exceptions_for_initial_data_load(self, spec_collection)

                spec_collection["doc_crud"][
                    MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col
                
                if self.alternate_load is True:
                    spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                    spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                    spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0

                self.log.info("Performing collection ops during swap rebalance...")
                for iterations in range(5):
                    collection_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        self.buckets_to_load,
                        spec_collection,
                        mutation_num=0,
                        batch_size=500,
                        process_concurrency=4)

                    if collection_task.result is True:
                        self.log.info("Iteration {0} of collection ops done".format(iterations+1))
                        spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                        spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                        spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0
                        self.sleep(5, "Wait for 5 seconds before starting with the next iteration")

        self.task_manager.get_task_result(rebalance_passed)
        if rebalance_passed.result is True:
            self.log.info("Swap Rebalance passed")
        else:
            self.log.info("Swap Rebalance failed")

        # VBuckets shuffling verification
        if CbServer.Services.KV in services_on_target_node:
            # Fetch vbucket stats after swap rebalance for verification
            shell = RemoteMachineShellConnection(self.spare_node)
            cbstats = Cbstats(shell)
            for vb_type in vb_types:
                vb_verification[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            shell.disconnect()

            # Check vbuckets are shuffled or not
            for vb_type in vb_types:
                if vb_details[vb_type].sort() \
                        != vb_verification[vb_type].sort():
                    self.log_failure("%s vbuckets shuffled post swap_rebalance"
                                     % vb_type)
                    self.log.error("%s vbuckets before vs after: %s != %s"
                                   % (vb_type,
                                      vb_details[vb_type],
                                      vb_verification[vb_type]))

        # Update master node
        self.cluster.master = self.spare_node
        self.cluster.nodes_in_cluster.append(self.spare_node)

        # Update spare_node to rebalanced-out node
        self.spare_node = node_to_upgrade
        self.cluster.nodes_in_cluster.remove(node_to_upgrade)

    def online_rebalance_out_in(self, node_to_upgrade, version,
                                install_on_spare_node=True):
        """
        cluster --OUT--> Node with previous version
        cluster <--IN-- Node with latest_build
        """

        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        # Rebalance-out the target_node
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of %s"
                             % node_to_upgrade.ip)
            return

        # Install target version on spare node
        if install_on_spare_node:
            self.install_version_on_node([self.spare_node], version)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats(self.cluster)

        # Update master node
        self.cluster.master = self.spare_node
        self.cluster.nodes_in_cluster.append(self.spare_node)

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade
        self.cluster.nodes_in_cluster.remove(node_to_upgrade)

    def online_rebalance_in_out(self, node_to_upgrade, version,
                                install_on_spare_node=True):
        """
        cluster <--IN-- Node with latest_build
        cluster --OUT--> Node with previous version
        """
        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        if install_on_spare_node:
            # Install target version on spare node
            self.install_version_on_node([self.spare_node], version)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])

        if self.upgrade_with_data_load and \
                self.cluster_supports_collections:
            spec_collection = self.bucket_util.get_crud_template_from_package(
                self.collection_spec)
            CollectionBase.over_ride_doc_loading_template_params(self, spec_collection)
            CollectionBase.set_retry_exceptions_for_initial_data_load(self, spec_collection)

            spec_collection["doc_crud"][
                    MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

            if self.alternate_load is True:
                spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0

            self.log.info("Performing collection ops during rebalance in...")
            for iterations in range(5):
                    collection_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        self.buckets_to_load,
                        spec_collection,
                        mutation_num=0,
                        batch_size=500,
                        process_concurrency=4)

                    if collection_task.result is True:
                        self.log.info("Iteration {0} of collection ops done".format(iterations+1))
                        spec_collection[MetaCrudParams.SCOPES_TO_DROP] = 0
                        spec_collection[MetaCrudParams.SCOPES_TO_RECREATE] = 0
                        spec_collection[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0
                        self.sleep(5, "Wait for 5 seconds before starting with the next iteration")

        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats(self.cluster)

        # Rebalance-out the target_node
        rest = self.__get_rest_node(self.spare_node)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        if self.upgrade_with_data_load and \
                self.cluster_supports_collections:
            self.log.info("Performing collection ops during rebalance out...")
            for iterations in range(2):
                    collection_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        self.buckets_to_load,
                        spec_collection,
                        mutation_num=0,
                        batch_size=500,
                        process_concurrency=4)

                    if collection_task.result is True:
                        self.log.info("Iteration {0} of collection ops done".format(iterations+1))
                        self.sleep(5, "Wait for 5 seconds before starting with the next iteration")
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Update master node
        self.cluster.master = self.spare_node
        self.cluster.nodes_in_cluster.append(self.spare_node)

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade
        self.cluster.nodes_in_cluster.remove(node_to_upgrade)

    def online_incremental(self, node_to_upgrade, version):
        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]
        # Rebalance-out the target_node
        rest = self.__get_rest_node(node_to_upgrade)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Install the required version on the node
        self.install_version_on_node([node_to_upgrade], version)

        # Rebalance-in the target_node again
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      node_to_upgrade.ip,
                      node_to_upgrade.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

    def failover_delta_recovery(self, node_to_upgrade):
        self.failover_recovery(node_to_upgrade, "delta")

    def failover_full_recovery(self, node_to_upgrade, graceful=True):
        self.failover_recovery(node_to_upgrade, "full", graceful)

    def offline(self, node_to_upgrade, version, rebalance_required=True):
        rest = RestConnection(node_to_upgrade)
        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.__get_build(version, shell)
        self.assertTrue(appropriate_build.url,
                        msg="Unable to find build %s" % version)
        self.assertTrue(shell.download_build(appropriate_build),
                        "Failed while downloading the build!")

        self.log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(
            appropriate_build, save_upgrade_config=False,
            forcefully=self.is_downgrade)
        shell.disconnect()
        if not upgrade_success:
            self.log_failure("Upgrade failed")
            return

        self.log.info("Wait for ns_server to accept connections")
        if not rest.is_ns_server_running(timeout_in_seconds=120):
            self.log_failure("Server not started post upgrade")
            return

        self.log.info("Validate the cluster rebalance status")
        if not rest.cluster_status()["balanced"]:
            if rebalance_required:
                otp_nodes = [node.id for node in rest.node_statuses()]
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_passed = rest.monitorRebalance()
                if not rebalance_passed:
                    self.log_failure(
                        "Rebalance failed post node upgrade of {0}"
                        .format(node_to_upgrade))
                    return
            else:
                self.log_failure(
                    "Cluster reported (/pools/default) balanced=false")
                return

    def full_offline(self, nodes_to_upgrade, version):
        for node in nodes_to_upgrade:
            rest = RestConnection(node)
            shell = RemoteMachineShellConnection(node)

            appropriate_build = self.__get_build(version, shell)
            self.assertTrue(appropriate_build.url,
                            msg="Unable to find build %s" % version)
            self.assertTrue(shell.download_build(appropriate_build),
                            "Failed while downloading the build!")

            self.log.info("Starting node upgrade")
            upgrade_success = shell.couchbase_upgrade(
                appropriate_build, save_upgrade_config=False,
                forcefully=self.is_downgrade)
            shell.disconnect()

            if upgrade_success:
                self.log.info("Upgrade of {0} completed".format(node))

            self.log.info("Wait for ns_server to accept connections")
            if not rest.is_ns_server_running(timeout_in_seconds=120):
                self.log_failure("Server not started post upgrade")
                return

        self.cluster_util.print_cluster_stats(self.cluster)

        rest = RestConnection(self.cluster.master)
        balanced = rest.cluster_status()["balanced"]

        if not balanced:
            self.log.info("Cluster not balanced. Rebalance starting...")
            otp_nodes = [node.id for node in rest.node_statuses()]
            rebalance_task = rest.rebalance(otpNodes=otp_nodes,
                                            ejectedNodes=[])
            if rebalance_task:
                self.log.info("Rebalance successful")
            else:
                self.log.info("Rebalance failed")

            self.cluster_util.print_cluster_stats(self.cluster)

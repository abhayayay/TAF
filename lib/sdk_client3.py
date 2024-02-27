#!/usr/bin/env python
"""
Python based SDK client interface
Created on oct 11, 2023

"""

import os
import subprocess
import time
from threading import Lock

import couchbase.exceptions
import couchbase.subdocument as SD
from acouchbase.management.collections import CollectionManager
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.logic.options import Compression, TLSVerifyMode
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.collection import Collection
from couchbase.exceptions import *
from couchbase.management.logic.collections_logic import CollectionSpec
from couchbase.result import MutateInResult, LookupInResult

from Cb_constants import ClusterRun, CbServer, DocLoading
from constants.sdk_constants.sdk_client_constants import SDKConstants
from global_vars import logger
from sdk_utils.python_sdk import SDKOptions


class SDKClientPool(object):
    """
    Client pool manager for list of SDKClients per bucket which can be
    reused / shared across multiple tasks
    """

    def __init__(self):
        self.log = logger.get("infra")
        self.clients = dict()

    def shutdown(self):
        """
        Shutdown all active clients managed by this ClientPool Object
        :return None:
        """
        self.log.debug("Closing clients from SDKClientPool")
        for bucket_name, bucket_dict in list(self.clients.items()):
            for client in bucket_dict["idle_clients"] \
                          + bucket_dict["busy_clients"]:
                client.close()
        self.clients = dict()

    def create_cluster_clients(
            self, cluster, servers, req_clients=1, username="Administrator",
            password="password", compression_settings=None):
        """
        Create set of clients for the specified cluster.
        All created clients will be saved under the respective cluster key.
        :param cluster: cluster object for which the clients will be created
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param req_clients: Required number of clients to be created for the
                            given bucket and client settings
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Same as expected by SDKClient class
        :return:
        """
        if cluster.name not in self.clients:
            self.clients[cluster.name] = dict()
            self.clients[cluster.name]["lock"] = Lock()
            self.clients[cluster.name]["idle_clients"] = list()
            self.clients[cluster.name]["busy_clients"] = list()

        for _ in range(req_clients):
            self.clients[cluster.name]["idle_clients"].append(SDKClient(
                servers, None,
                username=username, password=password,
                compression_settings=compression_settings))

    def get_cluster_client(self, cluster):
        """
        Method to get a cluster client which can be used for SDK operations
        further by a callee.
        :param cluster: Cluster object for which the client has to selected
        :return client: Instance of SDKClient object
        """
        client = None
        if not self.clients:
            return client
        while client is None:
            self.clients[cluster.name]["lock"].acquire()
            client = self.clients[cluster.name]["idle_clients"].pop()
            self.clients[cluster.name]["busy_clients"].append(client)
            self.clients[cluster.name]["lock"].release()
        return client

    def create_clients(self, bucket, servers,
                       req_clients=1,
                       username="Administrator", password="password",
                       compression_settings=None):
        """
        Create set of clients for the specified bucket and client settings.
        All created clients will be saved under the respective bucket key.

        :param bucket: Bucket object for which the clients will be created
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param req_clients: Required number of clients to be created for the
                            given bucket and client settings
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Same as expected by SDKClient class
        :return:
        """
        if bucket.name not in self.clients:
            self.clients[bucket.name] = dict()
            self.clients[bucket.name]["lock"] = Lock()
            self.clients[bucket.name]["idle_clients"] = list()
            self.clients[bucket.name]["busy_clients"] = list()

        for _ in range(req_clients):
            self.clients[bucket.name]["idle_clients"].append(SDKClient(
                servers, bucket,
                username=username, password=password,
                compression_settings=compression_settings))

    def get_client_for_bucket(self, bucket, scope=CbServer.default_scope,
                              collection=CbServer.default_collection):
        """
        API to get a client which can be used for SDK operations further
        by a callee.
        Note: Callee has to choose the scope/collection to work on
              later by itself.
        :param bucket: Bucket object for which the client has to selected
        :param scope: Scope name to select for client operation
        :param collection: Collection name to select for client operation
        :return client: Instance of SDKClient object
        """
        client = None
        col_name = scope + collection
        if bucket.name not in self.clients:
            return client
        while client is None:
            self.clients[bucket.name]["lock"].acquire()
            if col_name in self.clients[bucket.name]:
                # Increment tasks' reference counter using this client object
                client = self.clients[bucket.name][col_name]["client"]
                self.clients[bucket.name][col_name]["counter"] += 1
            elif self.clients[bucket.name]["idle_clients"]:
                client = self.clients[bucket.name]["idle_clients"].pop()
                client.select_collection(scope, collection)
                self.clients[bucket.name]["busy_clients"].append(client)
                # Create scope/collection reference using the client object
                self.clients[bucket.name][col_name] = dict()
                self.clients[bucket.name][col_name]["client"] = client
                self.clients[bucket.name][col_name]["counter"] = 1
            self.clients[bucket.name]["lock"].release()
        return client

    def release_client(self, client):
        """
        Release the acquired SDKClient object back into the pool
        :param client: Instance of SDKClient object
        :return None:
        """
        bucket = client.bucket
        if bucket.name not in self.clients:
            return
        col_name = client.scope_name + client.collection_name
        self.clients[bucket.name]["lock"].acquire()
        if self.clients[bucket.name][col_name]["counter"] == 1:
            self.clients[bucket.name].pop(col_name)
            self.clients[bucket.name]["busy_clients"].remove(client)
            self.clients[bucket.name]["idle_clients"].append(client)
        else:
            self.clients[bucket.name][col_name]["counter"] -= 1
        self.clients[bucket.name]["lock"].release()


class TransactionConfig(object):
    def __init__(self, durability=None, timeout=None,
                 cleanup_window=None, transaction_keyspace=None):
        """
        None means leave it to default value during config creation
        """
        self.durability = durability
        self.timeout = timeout
        self.cleanup_window = cleanup_window
        self.transaction_keyspace = transaction_keyspace


class SDKClient(object):
    """
    Python SDK Client Implementation for testrunner - master branch
    """
    sdk_connections = 0  # type: int
    sdk_disconnections = 0  # type: int

    @staticmethod
    def get_transaction_options(transaction_config_obj):
        transaction_options = TransactionOptions.transactionOptions()
        if transaction_config_obj.timeout is not None:
            transaction_options.timeout(Duration.ofSeconds(
                transaction_config_obj.timeout))
        if transaction_config_obj.durability is not None:
            transaction_options = transaction_options.durabilityLevel(
                KVDurabilityLevel.decodeFromManagementApi(
                    transaction_config_obj.durability))
        # transaction_options.metadataCollection(tnx_keyspace)
        return transaction_options

    def __init__(self, servers, bucket,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 username=None, password=None,
                 compression_settings=None, cert_path=None):
        """
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param bucket: Bucket object to which the SDK connection will happen
        :param scope:  Name of the scope to connect.
                       Default: '_default'
        :param collection: Name of the collection to connect.
                           Default: _default
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Dict of compression settings. Format:
                                     {
                                      "enabled": Bool,
                                      "minRatio": Double int (None to default),
                                      "minSize": int (None to default)
                                     }
        :param cert_path: Path of certificate file to establish connection
        """
        # Used during Cluster.connect() call
        self.hosts = list()

        # Used while creating connection for Cluster_run
        self.servers = list()

        self.scope_name = scope
        self.collection_name = collection
        self.username = username or servers[0].rest_username
        self.password = password or servers[0].rest_password
        self.default_timeout = 0
        self.cluster = None  # type: Cluster
        self.bucket = bucket
        self.bucketObj = None  # type: Bucket
        self.collection = None  # type: Collection
        self.compression = compression_settings
        self.cert_path = cert_path
        self.log = logger.get("test")
        if self.bucket is not None:
            if bucket.serverless is not None \
                    and bucket.serverless.nebula_endpoint:
                self.hosts = [bucket.serverless.nebula_endpoint.srv]
                self.log.info("For SDK, Nebula endpoint used for bucket is: %s"
                              % bucket.serverless.nebula_endpoint.ip)
        for server in servers:
            self.servers.append((server.ip, int(server.port)))
            if CbServer.use_https:
                self.scheme = "couchbases"
            else:
                self.scheme = "couchbase"
            if not ClusterRun.is_enabled:
                if server.type == "columnar":
                    self.hosts.append(server.ip + ":" + str(
                        16001))
                else:
                    self.hosts.append(server.ip)
        strt = time.time()
        self.__create_conn()
        if bucket is not None:
            self.log.debug("SDK connection to bucket: {} took {}s".format(
                bucket.name, time.time() - strt))
        SDKClient.sdk_connections += 1

    def __create_conn(self):
        if self.bucket:
            self.log.debug("Creating SDK connection for '%s'" % self.bucket)
        # Having 'None' will enable us to test without sending any
        # compression settings and explicitly setting to 'False' as well
        auth = PasswordAuthenticator(username=self.username,
                                     password=self.password)

        compression = None,  # type: Optional[Compression]
        compression_min_size = None,  # type: Optional[int]
        compression_min_ratio = None,  # type: Optional[float]

        cluster_env = None
        if self.compression is not None:
            if self.compression.get("enabled", False):
                compression = Compression(1)

            if "minSize" in self.compression:
                compression_min_size = self.compression["minSize"]

            if "minRatio" in self.compression:
                compression_min_ratio = self.compression["minRatio"]

        cluster_options = ClusterOptions(authenticator=auth,
                                         compression=compression,
                                         compression_min_size=compression_min_size,
                                         compression_min_ratio=compression_min_ratio,
                                         tls_verify=TLSVerifyMode('none'))
        # Set metadata-collection for storing transactional docs / subdocs
        # Default it uses _default collection
        # if self.transaction_conf.transaction_keyspace:
        #     b_name, scope, col = self.transaction_conf.transaction_keyspace
        #     tnx_keyspace = TransactionKeyspace.create(b_name, scope, col)
        #     trans_conf = trans_conf.metadataCollection(tnx_keyspace)
        #
        # t_cluster_env = t_cluster_env.transactionsConfig(trans_conf)

        i = 1
        while i <= 5:
            try:
                # Code for cluster_run
                if ClusterRun.is_enabled:
                    # master_seed = HashSet(Collections.singletonList(
                    #     SeedNode.create(
                    #         self.servers[0][0],
                    #         Optional.of(ClusterRun.memcached_port),
                    #         Optional.of(int(self.servers[0][1])))))
                    # self.cluster = Cluster.connect(master_seed,
                    #                                cluster_options)
                    pass
                else:
                    connection_string = "{0}://{1}".format(self.scheme, ", ".
                                                           join(self.hosts).
                                                           replace(" ", ""))

                    self.cluster = Cluster.connect(connection_string, cluster_options)
                break
            except AuthenticationException:
                self.log.error("Exception during cluster connection: " + (str(AuthenticationException)))
            except UnAmbiguousTimeoutException:
                self.log.error("Exception during cluster connection: " + (str(UnAmbiguousTimeoutException)))
            except Exception as e:
                self.log.error("Exception during cluster connection: " + str(e))
                i += 1

        if self.cluster is None:
            raise Exception("Cluster not able to connect")

        if self.bucket is not None:
            self.bucketObj = self.cluster.bucket(self.bucket.name)
            time.sleep(10)
            self.select_collection(self.scope_name,
                                   self.collection_name)

    def get_diagnostics_report(self):
        diagnostics_results = self.cluster.diagnostics()
        return diagnostics_results.as_json()

    def get_memory_footprint(self):
        out = subprocess.Popen(
            ['ps', 'v', '-p', str(os.getpid())],
            stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.log.info("RAM FootPrint: {}".format(str(mem)))
        return mem

    def close(self):
        self.log.debug("Closing SDK for bucket '%s'" % self.bucket)
        if self.cluster:
            try:
                self.cluster.close()
            except Exception as e:
                self.log.debug("exception while closing connection " + str(e))
            self.log.debug("Cluster disconnected and env shutdown")
            SDKClient.sdk_disconnections += 1

    # Scope/Collection APIs
    def collection_manager(self) -> CollectionManager:
        """
        :return collection_manager object:
        """
        return self.bucketObj.collections()

    @staticmethod
    def get_collection_spec(scope="_default", collection="_default") -> CollectionSpec:
        """
        Returns collection_spec object for further usage in tests.

        :param scope: - Name of the scope
        :param collection: - Name of the collection
        :return CollectionSpec object:
        """
        return CollectionSpec(collection_name=collection, scope_name=scope)

    def select_collection(self, scope_name, collection_name):
        """
        Method to select collection. Can be called directly from test case.
        """
        self.scope_name = scope_name
        self.collection_name = collection_name
        if collection_name != CbServer.default_collection:
            self.collection = self.bucketObj \
                .scope(scope_name) \
                .collection(collection_name)
        else:
            self.collection = self.bucketObj.default_collection()

    def create_scope(self, scope):
        """
        Create a scope using the given name
        :param scope: Scope name to be created
        """
        self.collection_manager().create_scope(scope)
        self.bucket.stats.increment_manifest_uid()

    def drop_scope(self, scope):
        """
        Drop a scope using the given name
        :param scope: Scope name to be dropped
        """
        self.collection_manager().drop_scope(scope)
        self.bucket.stats.increment_manifest_uid()

    def create_collection(self, collection, scope=CbServer.default_scope):
        """
        API to creae collection under a particular scope
        :param collection: Collection name to be created
        :param scope: Scope under which the collection is
                      going to be created
                      default: Cb_Server's default scope name
        """
        collection_spec = SDKClient.get_collection_spec(scope,
                                                        collection)
        self.collection_manager().create_collection(collection=collection_spec)
        self.bucket.stats.increment_manifest_uid()

    def drop_collection(self, scope=CbServer.default_scope,
                        collection=CbServer.default_collection):
        """
        API to drop the collection (if exists)
        :param scope: Scope name for targeted collection
                      default: Cb_Server's default scope name
        :param collection: Targeted collection name to drop
                           default: Cb_Server's default collection name
        """
        collection_spec = SDKClient.get_collection_spec(scope,
                                                        collection)
        self.collection_manager().drop_collection(collection=collection_spec)
        self.bucket.stats.increment_manifest_uid()

    # Singular CRUD APIs
    def delete(self, key, persist_to=None
               , replicate_to=None,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               durability=None, cas=0, sdk_retry_strategy=None):
        result = dict()
        result["cas"] = -1
        try:
            options = SDKOptions.get_remove_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas,
                sdk_retry_strategy=sdk_retry_strategy)
            delete_result = self.collection.remove(key, options)
            result.update({"key": key, "value": None,
                           "error": None, "status": True,
                           "cas": delete_result.cas})
        except DocumentNotFoundException as e:
            self.log.debug("Exception: Document id {0} not found - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CasMismatchException as e:
            self.log.debug("Exception: Cas mismatch for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CouchbaseException as e:
            self.log.debug("CB generic exception for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as e:
            self.log.error("Error during remove of {0} - {1}".format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        return result

    def insert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=None,
               replicate_to=None,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               doc_type="json",
               durability=None, sdk_retry_strategy=None):

        result = dict()
        result["cas"] = 0

        try:
            options = SDKOptions.get_insert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                sdk_retry_strategy=sdk_retry_strategy, doc_type=doc_type)

            insert_result = self.collection.insert(key, value, options)
            result.update({"key": key, "value": value,
                           "error": None, "status": True,
                           "cas": insert_result.cas})
        except DocumentExistsException as ex:
            self.log.debug("The document already exists! => " + str(ex))
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except DurabilityImpossibleException as ex:
            self.log.debug("Durability impossible for key: " + str(ex))
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except DurabilitySyncWriteAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except InternalServerFailureException as ex:
            self.log.debug("OOM exception: %s" % ex)
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except CouchbaseException as e:
            self.log.debug("CB generic exception for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        return result

    def replace(self, key, value,
                exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                persist_to=None
                , replicate_to=None,
                timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                durability=None, cas=0, sdk_retry_strategy=None,
                preserve_expiry=None):
        result = dict()
        result["cas"] = 0
        content = value
        try:
            options = SDKOptions.get_replace_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy)
            # Returns com.couchbase.client.java.kv.MutationResult object
            replace_result = self.collection.replace(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": replace_result.cas})
        except DocumentExistsException as ex:
            self.log.debug("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except CasMismatchException as e:
            self.log.debug("CAS mismatch for key %s - %s" % (key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DocumentNotFoundException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DurabilitySyncWriteAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except FeatureUnavailableException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def touch(self, key, exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
              persist_to=None
              , replicate_to=None,
              durability=None,
              timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
              sdk_retry_strategy=None):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None
        }
        touch_options = SDKOptions.get_touch_options(
            timeout, time_unit,
            sdk_retry_strategy=sdk_retry_strategy)
        try:
            touch_result = self.collection.touch(
                key,
                SDKOptions.get_duration(exp, exp_unit),
                touch_options)
            result.update({"status": True, "cas": touch_result.cas})
        except DocumentNotFoundException as e:
            self.log.debug("Document key '%s' not found!" % key)
            result["error"] = str(e)
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except FeatureUnavailableException as ex:
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def read(self, key, timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
             sdk_retry_strategy=None, populate_value=True, with_expiry=None):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None,
            "ttl_present": None
        }
        read_options = SDKOptions.get_read_options(
            timeout, time_unit,
            sdk_retry_strategy=sdk_retry_strategy)
        try:
            get_result = self.collection.get(key, read_options)
            result["status"] = True
            result["value"] = get_result.content_as
            result["cas"] = get_result.cas
        except DocumentNotFoundException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except CouchbaseException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def get_from_all_replicas(self, key):
        result = []
        get_result = self.collection.get_all_replicas(key)
        try:
            for item in get_result:
                result.append({"key": key,
                               "value": item.content_as,
                               "cas": item.cas, "status": True})
        except Exception:
            pass
        return result

    def upsert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=None
               , replicate_to=None,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               durability=None, sdk_retry_strategy=None, preserve_expiry=None):
        content = value
        result = dict()
        result["cas"] = 0
        try:
            options = SDKOptions.get_upsert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy)
            upsert_result = self.collection.upsert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": upsert_result.cas})
        except DocumentExistsException as ex:
            self.log.debug("Upsert: Document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except DurabilitySyncWriteAmbiguousException as ex:
            self.log.debug("Durability Ambiguous for key: %s" % key)
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except CouchbaseException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def crud(self, op_type, key, value=None, exp=0, replicate_to=None,
             persist_to=None, durability=None,
             timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
             create_path=True, xattr=False, cas=0, sdk_retry_strategy=None,
             store_semantics=None, preserve_expiry=None, access_deleted=False,
             create_as_deleted=False):
        result = None
        if op_type == DocLoading.Bucket.DocOps.UPDATE:
            result = self.upsert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry)
        elif op_type == DocLoading.Bucket.DocOps.CREATE:
            result = self.insert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            result = self.delete(
                key,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.REPLACE:
            result = self.replace(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                cas=cas,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry)
        elif op_type == DocLoading.Bucket.DocOps.TOUCH:
            result = self.touch(
                key, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.READ:
            result = self.read(
                key, timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type in [DocLoading.Bucket.SubDocOps.INSERT, "subdoc_insert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]

            mutate_in_specs = list()

            mutate_in_specs.append(SD.insert(path=sub_key,
                                             value=value,
                                             create_parents=create_path,
                                             xattr=xattr))

            if not xattr:
                mutate_in_specs.append(SD.counter(path="mutated",
                                                  delta=1))

            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted, cas=cas)

            exp = None  # type: Exception
            try:
                result = self.collection.mutate_in(key, mutate_in_specs, options)
            except Exception as e:
                exp = e
            finally:
                return self.__transate_sub_doc_result(data=result, key=key, exp=exp, path_val=path_val)

        elif op_type in [DocLoading.Bucket.SubDocOps.UPSERT, "subdoc_upsert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]

            mutate_in_specs = list()

            mutate_in_specs.append(SD.upsert(path=sub_key,
                                             value=value,
                                             create_parents=create_path,
                                             xattr=xattr))

            if not xattr:
                mutate_in_specs.append(SD.counter(path="mutated",
                                                  delta=1))

            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted, cas=cas)

            exp = None  # type: Exception
            try:
                result = self.collection.mutate_in(key, mutate_in_specs, options)
            except Exception as e:
                exp = e
            finally:
                return self.__transate_sub_doc_result(data=result, key=key, exp=exp, path_val=path_val)

        elif op_type == "subdoc_replace":
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]

            mutate_in_specs = list()

            mutate_in_specs.append(SD.replace(path=sub_key,
                                              value=value,
                                              xattr=xattr))

            if not xattr:
                mutate_in_specs.append(SD.counter(path="mutated",
                                                  delta=1))

            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted, cas=cas)

            exp = None  # type: Exception
            try:
                result = self.collection.mutate_in(key, mutate_in_specs, options)
            except Exception as e:
                exp = e
            finally:
                return self.__transate_sub_doc_result(data=result, key=key, exp=exp, path_val=path_val)

        elif op_type in [DocLoading.Bucket.SubDocOps.REMOVE, "subdoc_delete"]:

            path_val = dict()
            path_val[key] = [(value, "")]

            mutate_in_specs = list()

            mutate_in_specs.append(SD.remove(path=value,
                                             xattr=xattr))

            if not xattr:
                mutate_in_specs.append(SD.counter(path="mutated",
                                                  delta=1))

            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted, cas=cas)

            exp = None  # type:Exception
            try:
                result = self.collection.mutate_in(key, mutate_in_specs, options)
            except Exception as e:
                exp = e
            finally:
                return self.__transate_sub_doc_result(data=result, key=key, exp=exp, path_val=path_val)

        elif op_type in [DocLoading.Bucket.SubDocOps.LOOKUP, "subdoc_read"]:
            path_val = dict()
            path_val[key] = [(value, '')]

            lookup_in_specs = list()

            lookup_in_specs.append(SD.get(path=value,
                                          xattr=xattr))

            exp = None  # type: Exception
            try:
                result = self.collection.lookup_in(key, lookup_in_specs)
            except Exception as e:
                exp = e
            finally:
                return self.__translate_get_multi_sub_doc_result(data=result, key=key, exp=exp, path_val=path_val)

        elif op_type == DocLoading.Bucket.SubDocOps.COUNTER:
            sub_key, step_value = value[0], value[1]

            mutate_in_specs = list()

            if not xattr:
                mutate_in_specs.append(SD.counter(path=sub_key,
                                                  delta=value,
                                                  xattr=xattr,
                                                  create_parents=create_path))

            if not xattr:
                mutate_in_specs.append(SD.counter(path="mutated",
                                                  delta=1))

            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted, cas=cas)

            exp = None  # type: Exception
            try:
                result = self.collection.mutate_in(key, mutate_in_specs, options)
            except Exception as e:
                exp = e
            finally:
                return self.__transate_sub_doc_result(data=result, key=key, exp=exp, path_val={})
        else:
            self.log.error("Unsupported operation %s" % op_type)
        return result

    def insert_binary_document(self, keys, sdk_retry_strategy=None):
        options = \
            SDKOptions.get_insert_options(doc_type="binary")
        for key in keys:
            binary_value = ''.join(format(i, '08b') for i in bytearray('{value":"' + key + '"}', encoding='utf-8'))

            self.collection.insert(key, binary_value, options)

    def insert_string_document(self, keys, sdk_retry_strategy=None):
        options = \
            SDKOptions.get_upsert_options(doc_type='string')
        for key in keys:
            self.collection.upsert(key, '{value":"' + key + '"}', options)

    def insert_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_insert",
                  document_id,
                  [path, value],
                  time_unit=SDKConstants.TimeUnit.SECONDS,
                  create_path=create_parents,
                  xattr=xattr)

    def update_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_upsert",
                  document_id,
                  [path, value],
                  time_unit=SDKConstants.TimeUnit.SECONDS,
                  create_path=create_parents,
                  xattr=xattr)

    def run_query(self, query, timeout=None,
                  timeunit=SDKConstants.TimeUnit.SECONDS):
        if timeout is not None:
            options = QueryOptions(timeout=SDKOptions.get_duration(timeout,
                                                                   timeunit))
            return self.cluster.query(query, options)
        return self.cluster.query(query)

    def __transate_sub_doc_result(self, data: MutateInResult, key='', exp: Exception = None,
                                  path_val: dict = {}):
        success = dict()
        fail = dict()

        if exp is not None:
            fail[key] = dict()
            fail[key]['cas'] = 0
            fail[key]['error'] = str(exp)
            return success, fail

        if exp is None and data.success:
            success[key] = dict()
            success[key]['cas'] = data.cas
            success[key]['value'] = data.content_as
        else:
            fail[key] = dict()
            fail[key]['error'] = str(exp)
            fail[key]['cas'] = 0
            success[key]['value'] = dict()
            fail[key]['path_val'] = path_val[key]

        return success, fail

    def __translate_get_multi_sub_doc_result(self, data: LookupInResult, key='', exp: Exception = None,
                                             path_val: dict = {}):
        success = dict()
        fail = dict()

        if exp is not None:
            fail[key] = dict()
            fail[key]['cas'] = 0
            fail[key]['error'] = str(exp)
            return success, fail

        if exp is None and data.success and data.exists(0):
            success[key] = dict()
            success[key]['cas'] = data.cas
            success[key]['value'] = data.content_as
        else:
            fail[key] = dict()
            fail[key]['error'] = str(couchbase.exceptions.PathNotFoundException) or exp
            fail[key]['cas'] = 0
            fail[key]['value'] = dict()
            fail[key]['path_val'] = path_val[key]

        return success, fail

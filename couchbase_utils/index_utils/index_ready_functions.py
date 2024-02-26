'''
Created on 07-May-2021

@author: riteshagarwal
'''
from Cb_constants import CbServer
from global_vars import logger
import time
from membase.api.rest_client import RestConnection
from gsiLib.GsiHelper_Rest import GsiHelper
import string, random, math


class IndexUtils:

    def __init__(self, server_task=None):
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.log = logger.get("test")

    """
    Method to create list of RestClient for each node
    arguments: 
             a. nodes_list: list of nodes list
    return: list of restClient object corresponds each node in the list
    """

    def create_restClient_obj_list(self, nodes_list):
        restClient_list = list()
        for query_node in nodes_list:
            restClient_list.append(RestConnection(query_node))
        return restClient_list

    def build_deferred_indexes(self, cluster, indexes_to_build):
        """
        Build secondary indexes that were deferred
        """
        query_nodes_count = len(cluster.query_nodes)
        restClient_Obj_list = self.create_restClient_obj_list(cluster.query_nodes)
        self.log.info("Building indexes")
        x = 0
        for bucket, bucket_data in list(indexes_to_build.items()):
            for scope, collection_data in list(bucket_data.items()):
                for collection, gsi_index_names in list(collection_data.items()):
                    build_query = "BUILD INDEX on `%s`.`%s`.`%s`(%s) " \
                                  "USING GSI" \
                                  % (bucket, scope, collection, gsi_index_names)
                    self.log.debug("Query is: {}".format(build_query))
                    query_client = restClient_Obj_list[x % query_nodes_count]
                    json_load = query_client.query_tool(build_query)
                    self.log.debug("Json load for deferred build is: {}".format(json_load))
                    x += 1

    def create_gsi_on_each_collection(self, cluster, buckets=None, gsi_base_name=None,
                                      replica=0, defer=True, number_of_indexes_per_coll=1, count=0,
                                      field='key', sync=False, timeout=600, retry=3):
        """
        Create gsi indexes on collections - according to number_of_indexes_per_coll
        """
        self.log.info("Creating indexes with defer:{} build".format(defer))
        if buckets is None:
            buckets = cluster.buckets

        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        query_node_list = cluster.query_nodes
        query_nodes_count = len(query_node_list)
        x = 0
        createIndexTasklist = list()
        indexes_to_build = dict()
        counter = 0
        for bucket in couchbase_buckets:
            if bucket.name not in indexes_to_build:
                indexes_to_build[bucket.name] = dict()
            for _, scope in list(bucket.scopes.items()):
                if scope.name == CbServer.system_scope or scope.name == CbServer.default_scope:
                    continue
                if scope.name not in indexes_to_build[bucket.name]:
                    indexes_to_build[bucket.name][scope.name] = dict()
                for _, collection in list(scope.collections.items()):
                    for tempCount in range(count, number_of_indexes_per_coll):
                        if gsi_base_name is None:
                            gsi_index_name = bucket.name.replace(".", "") + "_" + scope.name + "_" +\
                                             collection.name + "_" + str(tempCount)
                        else:
                            gsi_index_name = gsi_base_name + str(counter)
                            counter += 1

                        if replica > 0:
                            create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`%s`) " \
                                             "WITH { 'defer_build': %s, 'num_replica': %s }" \
                                             % (gsi_index_name, bucket.name,
                                                scope.name, collection.name, field,
                                                defer, replica)
                        else:
                            create_index_query = "CREATE INDEX `%s` " \
                                                 "ON `%s`.`%s`.`%s`(`%s`) " \
                                                 "WITH { 'defer_build': %s}" \
                                                 % (gsi_index_name, bucket.name,
                                                    scope.name, collection.name, field,
                                                    defer)
                        query_node_instance = x % query_nodes_count
                        x = x + 1
                        self.log.debug("sending query:"+create_index_query)
                        self.log.debug("Sending index name:"+gsi_index_name)
                        task = self.task.async_execute_query(server=query_node_list[query_node_instance],
                                                             query=create_index_query,
                                                             isIndexerQuery=not defer, bucket=bucket,
                                                             indexName=gsi_index_name, timeout= timeout, retry=retry)
                        if sync:
                            self.task_manager.get_task_result(task)
                        else:
                            createIndexTasklist.append(task)
                        if collection.name not in indexes_to_build[bucket.name][scope.name]:
                            indexes_to_build[bucket.name][scope.name][collection.name] = list()
                        indexes_to_build[bucket.name][scope.name][collection.name].append(gsi_index_name)

        return indexes_to_build, createIndexTasklist

    def recreate_dropped_indexes(self, cluster, indexes_dropped, field='body', defer=True, replica=0, timeout=600, retry=3):
        """
        Recreate dropped indexes given indexes_dropped dict
        """
        self.log.info("Recreating dropped indexes")
        query_node_list = cluster.query_nodes
        query_nodes_count = len(query_node_list)
        x = 0
        buckets = cluster.buckets
        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        for bucket in couchbase_buckets:
            for _, scope in list(bucket.scopes.items()):
                if scope.name == CbServer.system_scope or scope.name == CbServer.default_scope:
                    continue
                for _, collection in list(scope.collections.items()):
                    gsi_index_names = indexes_dropped[bucket.name][scope.name][collection.name]
                    for gsi_index_name in list(gsi_index_names):
                        if replica > 0:
                            create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`%s`) " \
                                             "WITH { 'defer_build': %s, 'num_replica': %s }" \
                                             % (gsi_index_name, bucket.name,
                                                scope.name, collection.name, field,
                                                defer, replica)
                        else:
                            create_index_query = "CREATE INDEX `%s` " \
                                                 "ON `%s`.`%s`.`%s`(`%s`) " \
                                                 "WITH { 'defer_build': %s}" \
                                                 % (gsi_index_name, bucket.name,
                                                    scope.name, collection.name, field,
                                                    defer)
                        query_node_instance = x % query_nodes_count
                        self.log.debug("sending query for recreate:" + create_index_query)
                        self.log.debug("Sending index name for recreate:" + gsi_index_name)
                        x = x + 1
                        task = self.task.async_execute_query(server=query_node_list[query_node_instance],
                                                             query=create_index_query,
                                                             isIndexerQuery=not defer, bucket=bucket,
                                                             indexName=gsi_index_name, timeout=timeout, retry=retry)
                        self.task_manager.get_task_result(task)

    def async_drop_indexes(self, cluster, indexList, buckets=None, drop_only_given_indexes=False):
        """
        Drop gsi indexes
        Returns dropped indexes dict and task list
        """
        indexes_dropped = dict()
        if buckets is None:
            buckets = cluster.buckets
        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        query_nodes_list = cluster.query_nodes
        query_nodes_count = len(query_nodes_list)
        x = 0
        dropIndexTaskList = list()
        for bucket in couchbase_buckets:
            indexes_dropped[bucket.name] = dict()
            for scope_name, scope in list(bucket.scopes.items()):
                if scope_name == '_default' or scope_name == '_system':
                    continue
                if drop_only_given_indexes:
                    if scope.name not in list(indexList[bucket.name].keys()):
                        continue
                indexes_dropped[bucket.name][scope.name] = dict()
                for _, collection in list(scope.collections.items()):
                    gsi_index_names = indexList[bucket.name][scope.name][collection.name]
                    for gsi_index_name in list(gsi_index_names):
                        drop_index_query = "DROP INDEX `%s` ON " \
                                           "`%s`.`%s`.`%s`" \
                                           "USING GSI" \
                                           % (gsi_index_name, bucket, scope, collection)
                        self.log.debug("Drop query is {}".format(drop_index_query))
                        query_node_index = x % query_nodes_count
                        task = self.task.async_execute_query(server=query_nodes_list[query_node_index],
                                                             query=drop_index_query,
                                                             bucket=bucket,
                                                             indexName=gsi_index_name, isIndexerQuery=False)
                        dropIndexTaskList.append(task)
                        gsi_index_names.remove(gsi_index_name)
                        if collection.name not in indexes_dropped[bucket.name][scope.name]:
                            indexes_dropped[bucket.name][scope.name][collection.name] = list()
                        indexes_dropped[bucket.name][scope.name][collection.name].append(gsi_index_name)
                        x += 1
        return dropIndexTaskList, indexes_dropped

    def alter_indexes(self, cluster, indexesDict, num_replicas = 1):
        alter_index_task_info = list()
        x = 0
        query_len = len(cluster.query_nodes)
        for bucket, bucket_data in list(indexesDict.items()):
            for scope, collection_data in list(bucket_data.items()):
                for collection, gsi_index_names in list(collection_data.items()):
                    for gsi_index_name in gsi_index_names:
                        full_keyspace_name = "default:`" + bucket + "`.`" + scope + "`.`" + \
                                             collection + "`.`" + gsi_index_name + "`"
                        query_node_index = x % query_len
                        query = "ALTER INDEX %s WITH {\"action\": \"replica_count\", \"num_replica\": %s}" % (
                        full_keyspace_name, num_replicas)
                        self.log.debug("Alter index query is {}".format(query))
                        task = self.task.async_execute_query(cluster.query_nodes[query_node_index], query,
                                                             isIndexerQuery=False)
                        alter_index_task_info.append(task)
                        x += 1
        return alter_index_task_info

    def delete_docs_with_field(self, cluster, indexMap, field='body', sync=True):
        """
        Drop gsi indexes
        Returns dropped indexes dict and task list
        """

        self.log.info("Deleting documents")
        x = 0
        query_len = len(cluster.query_nodes)
        for bucket, bucket_data in list(indexMap.items()):
            for scope, collection_data in list(bucket_data.items()):
                for collection, gsi_index_names in list(collection_data.items()):
                    query = "delete from `%s`.`%s`.`%s` where %s is not missing;" % (
                            bucket, scope, collection, field)
                    query_node_index = x % query_len
                    task = self.task.async_execute_query(cluster.query_nodes[query_node_index], query)
                    if sync:
                        self.task_manager.get_task_result(task)
                    x += 1


    def run_cbq_query(self, query, n1ql_node=None, timeout=1300):
        """
        To run cbq queries
        Note: Do not run this in parallel
        """
        n1ql_node = n1ql_node or self.n1ql_node
        conn = RestConnection(n1ql_node)
        result = conn.query_tool(query, timeout)
        return result

    def wait_for_indexes_to_go_online(self, cluster, buckets, gsi_index_name, timeout=600, sleep_time=10):
        """
        Wait for indexes to go online after building the deferred indexes
        """
        self.log.info("Waiting for indexes to go online")
        start_time = time.time()
        stop_time = start_time + timeout
        self.indexer_rest = GsiHelper(cluster.master, self.log)
        for bucket in buckets:
            bucket_name = bucket.name.replace(".", "")
            start = gsi_index_name.find(bucket_name)
            if start > -1:
                if gsi_index_name[start + len(bucket_name)] == '_':
                    while True:
                        if self.indexer_rest.polling_create_index_status(bucket=bucket, index=gsi_index_name, timeout=timeout, sleep_time=sleep_time) is True:
                            return True
                        else:
                            if time.time() > stop_time:
                                return False

    def randStr(self, Num=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(Num))

    def filter_buckets(self, bucket_list, indexMap):
        newIndexMap = dict()
        for bucket in bucket_list:
            newIndexMap[bucket.name] = indexMap[bucket.name]
        return newIndexMap


    def run_full_scan(self, cluster, indexesDict, key, totalCount, bucket_list=None, limit=1000000, is_sync=True, 
                      offSetBound=0, lightLoad=False, numThreadsPerNode=1):
        reference_dict = dict()
        if bucket_list is None:
            self.log.info("bucket list is None")
            reference_dict = indexesDict
        else:
            self.log.info("bucket list is not None")
            reference_dict = self.filter_buckets(bucket_list, indexesDict)

        query_tasks_info = list()
        x = 0
        query_len = len(cluster.query_nodes)
        self.log.debug("Limit is {} and total Count is {}".format(limit, totalCount))
        for bucket, bucket_data in list(reference_dict.items()):
            for scope, collection_data in list(bucket_data.items()):
                for collection, gsi_index_names in list(collection_data.items()):
                    for gsi_index_name in gsi_index_names:
                        offset = offSetBound
                        while True:
                            query_node_index = x % query_len
                            if not lightLoad:
                                query = "select * from `%s`.`%s`.`%s` data USE INDEX (%s USING GSI) where %s is not missing order by meta().id limit %s offset %s" % (
                                    bucket, scope, collection, gsi_index_name, key, limit, offset)
                            else:
                                query = "select meta().id from `%s`.`%s`.`%s` data USE INDEX (%s USING GSI) where %s is not missing order by meta().id limit %s offset %s" % (
                                    bucket, scope, collection, gsi_index_name, key, limit, offset)
                            self.log.debug("Query is {}".format(query))
                            self.log.debug("Offset is {} ".format(offset))
                            task = self.task.async_execute_query(cluster.query_nodes[query_node_index], query)
                            query_tasks_info.append(task)
                            x += 1
                            if is_sync:
                                self.log.debug("Is sync is true")
                                if x == numThreadsPerNode * query_len:
                                    self.log.debug("Getting status for each query")
                                    for task in query_tasks_info:
                                        self.task_manager.get_task_result(task)
                                    self.log.debug("Resetting the list")
                                    query_tasks_info = list()
                                    x = 0
                            offset += limit
                            if offset > totalCount:
                                break
        return query_tasks_info

    def get_indexer_mem_quota(self, indexer_node):
        """
        Get Indexer memory Quota
        :param indexer_node:
        """
        rest = RestConnection(indexer_node)
        content = rest.cluster_status()
        return int(content['indexMemoryQuota'])

'''
Created on 31-May-2021

@author: riteshagarwal
'''
import os

from global_vars import logger
from remote.remote_util import RemoteMachineShellConnection


class MagmaUtils:
    def __init__(self):
        self.log = logger.get("test")

    def get_disk_usage(self, cluster, bucket, data_path, servers=None):
        """ Returns magma disk usage.

        Args:
            cluster (Cluster): The TAF cluster object.
            bucket (Bucket): The bucket who's disk usage is being fetched.
            data_path (str): A path to Couchbase's data directory.
            server (list(TestInputServer)): The servers to fetch the usage from.

        Returns:
            list: A list of disk usage by magma component (e.g. [kvstore,
            write-ahead-log, key-tree and seq-tree])
        """
        disk_usage = []
        if servers is None:
            servers = cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(data_path,
                             bucket.name, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(data_path,
                             bucket.name, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(data_path,
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(data_path,
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.info("Disk usage stats for bucekt {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        disk_usage.extend([kvstore, wal, keyTree, seqTree])
        return disk_usage
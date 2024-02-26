import logging

logger = dict()
logger["infra"] = logging.getLogger("infra")
logger["test"] = logging.getLogger("test")
logging.getLogger("paramiko").setLevel(logging.WARN)

system_event_logs = None

cluster_util = bucket_util = serverless_util = None

#!/usr/bin/env python

import queue
import logging.config
import sys
import threading
import time
import traceback

from custom_exceptions.exception import InstallException
from . import install_constants
from . import install_utils


sys.path = [".", "lib", "platform_utils", "scripts"] + sys.path
logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()


def node_installer(node, install_tasks):
    while True:
        install_task = install_tasks.get()
        if install_task is None:
            break
        else:
            do_install_task(install_task, node)
            install_tasks.task_done()


def on_install_error(install_task, node, e):
    node.queue.empty()
    log.error("Error {0}:{1} occurred on {2} during {3}"
              .format(repr(e), e, node.ip, install_task))


def do_install_task(task, node):
    try:
        if task == "uninstall":
            node.uninstall_cb()
        elif task == "install":
            node.install_cb()
        elif task == "init":
            node.init_cb()
        elif task == "cleanup":
            node.cleanup_cb()
        log.info("Done with %s on %s." % (task, node.ip))
    except Exception as e:
        on_install_error(task, node, e)
        traceback.print_exc()


def validate_install(version):
    log.info("-" * 100)
    for node in install_utils.NodeHelpers:
        node.install_success = False
        if node.rest:
            try:
                node_status = node.rest.cluster_status()["nodes"]
            except Exception:
                continue
            for item in node_status:
                if version in item['version'] and item['status'] == "healthy":
                    node.install_success = True

                if node.enable_ipv6 and not item["addressFamily"] == "inet6":
                    node.install_success = False

                afamily = "Unknown"
                if 'addressFamily' in list(item.keys()):
                    afamily = item['addressFamily']

                log.info("node:{0}\tversion:{1}\taFamily:{2}\tservices:{3}"
                         .format(item['hostname'],
                                 item['version'],
                                 afamily,
                                 item['services']))
    install_utils.print_result_and_exit()


def do_install(params):
    # Per node, spawn one thread, which will process a queue of install tasks
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        install_tasks = params["install_tasks"]
        queue_obj = queue.Queue()
        for task in install_tasks:
            queue_obj.put(task)
        t = threading.Thread(target=node_installer,
                             args=(node_helper, queue_obj))
        t.daemon = True
        t.start()
        node_helper.queue = queue_obj
        node_helper.thread = t

    force_stop = start_time + params["timeout"]
    for node in install_utils.NodeHelpers:
        try:
            while node.queue.unfinished_tasks and time.time() < force_stop:
                time.sleep(install_constants.INSTALL_POLL_INTERVAL)
            else:
                raise InstallException
        except InstallException:
            if time.time() >= force_stop:
                log.error("INSTALL TIMED OUT AFTER {0}s.VALIDATING.."
                          .format(params["timeout"]))
                break
    if "init" in params["install_tasks"]:
        validate_install(params["version"])


def main():
    params = install_utils.process_user_input()
    install_utils.pre_install_steps()
    do_install(params)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("TOTAL INSTALL TIME = {0} seconds"
             .format(round(end_time - start_time)))
    sys.exit(0)

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from common_lib import sleep
from global_vars import logger
from tasks.gracefull_killer import GracefulKiller


class TaskManager(GracefulKiller):
    def __init__(self, number_of_threads: int = 10) -> None:
        super().__init__()
        self.log = logger.get("infra")
        self.log.info("Initiating TaskManager with %d threads"
                      % number_of_threads)

        # number_of_threads + 1, extra thread to kill TaskManager upon SIGTERM

        self.number_of_threads = number_of_threads + 1
        self.pool = ThreadPoolExecutor(self.number_of_threads)
        self.killer_manager_future = self.pool.submit(self.task_manager_killer)
        self.futures = dict()

    def task_manager_killer(self):
        try:
            while not self.kill_now:
                time.sleep(5)
            self.log.info("Task Manager going to killed gracefully")
            self.shutdown(force=True)
            self.abort_all_tasks()
        except KeyboardInterrupt:
            self.log.info("interrupt in task_manager_killer thread")
            self.log.info("Task Manager going to killed gracefully")
            self.shutdown(force=True)
            self.abort_all_tasks()

    def add_new_task(self, task) -> None:
        future = self.pool.submit(task.call)
        self.futures[task.thread_name] = future
        self.log.info("Added new task: %s" % task.thread_name)

    def get_task_result(self, task) -> None:
        self.log.debug("Getting task result for %s" % task.thread_name)
        future = self.futures[task.thread_name]
        result = False
        try:
            result = future.result()
        except Exception as e:
            self.log.warning("%s is already cancelled" % task.thread_name)
            raise e

        self.futures.pop(task.thread_name)
        return result

    def schedule(self, task, sleep_time: int = 0) -> None:
        if sleep_time > 0:
            sleep(sleep_time,
                  "Wait before scheduling task %s" % task.thread_name,
                  log_type="infra")
        self.add_new_task(task)

    def stop_task(self, task) -> None:
        if task.thread_name not in list(self.futures.keys()):
            return
        future = self.futures[task.thread_name]
        i = 0
        while not future.done() and i < 30:
            sleep(1,
                  "Wait for %s to complete. Current status: %s"
                  % (task.thread_name, future.done()),
                  log_type="infra")
            i += 1
        else:
            self.log.debug("Task %s in already finished. No need to stop task"
                           % task.thread_name)
        if not future.done():
            self.log.debug("Stopping task %s" % task.thread_name)
            future.cancel()

    def shutdown_task_manager(self, timeout=5) -> None:
        try:
            self.exit_gracefully()
            self.log.debug("killer_manager is killed")
        except Exception as e:
            self.log.info(str(e))
        finally:
            self.shutdown()

    def shutdown(self, force=False) -> None:
        self.log.info("Running TaskManager shutdown")
        try:
            # Shutting down the task manager after all task's finishes.
            if force:
                self.pool.shutdown(wait=False, cancel_futures=True)
            else:
                self.pool.shutdown(wait=False)
        except Exception as ex:
            self.log.error(ex)
            self.pool.shutdown(wait=False, cancel_futures=True)

    def print_tasks_in_pool(self) -> None:
        for task_name, future in list(self.futures.items()):
            if not future.done():
                self.log.warning("Task '%s' not completed" % task_name)

    def abort_all_tasks(self) -> None:
        for task, future in list(self.futures.items()):
            if future.done():
                self.log.debug("Completed task {0}".format(task))
            else:
                self.log.info("Stopping task {0}".format(task))
                future.cancel()
                self.futures.pop(task)
        self.log.info("all tasks are aborted")

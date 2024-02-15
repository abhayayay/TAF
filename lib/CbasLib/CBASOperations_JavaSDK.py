"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import sys
import traceback

from com.couchbase.client.java.analytics import AnalyticsQuery, AnalyticsParams
from com.couchbase.client.core import RequestCancelledException, \
                                      CouchbaseException
from java.lang import System, RuntimeException
from java.util.concurrent import TimeoutException, RejectedExecutionException,\
                                 TimeUnit
from CbasLib.CBASOperations_Rest import CBASHelper as CBAS_helper_rest
from common_lib import sleep
from sdk_client3 import SDKClient


class CBASHelper(CBAS_helper_rest, SDKClient):
    def __init__(self, master, cbas_node):
        self.server = master
        super(CBASHelper, self).__init__(master, cbas_node)
        SDKClient(self.cluster, None, [self.server]).__init__(
            self.cluster, None, [self.server])
        self.connectionLive = False

    def createConn(self, bucket, username=None, password=None):
        if username:
            self.username = username
        if password:
            self.password = password

        self.connectCluster(username, password)
        System.setProperty("com.couchbase.analyticsEnabled", "true")
        self.bucket = self.cluster.openBucket(bucket)
        self.connectionLive = True

    def closeConn(self):
        if self.connectionLive:
            try:
                self.bucket.close()
                self.disconnectCluster()
                self.connectionLive = False
            except CouchbaseException as e:
                # Wait before closing bucket connection
                sleep(5)
                try:
                    self.bucket.close()
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                self.log.error("%s" % e)
                traceback.print_exception(*sys.exc_info())
            except TimeoutException as e:
                # Wait before closing bucket connection
                sleep(5)
                try:
                    self.bucket.close()
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                self.log.error("%s" % e)
                traceback.print_exception(*sys.exc_info())
            except RuntimeException as e:
                sleep(5, "RuntimeException from Java SDK: %s" % str(e),
                      log_type="infra")
                try:
                    self.bucket.close()
                except:
                    pass
                self.disconnectCluster()
                self.connectionLive = False
                self.log.error("%s" % e)
                traceback.print_exception(*sys.exc_info())

    def execute_statement_on_cbas(self, statement, mode, pretty=True,
                                  timeout=70, client_context_id=None,
                                  username=None, password=None,
                                  analytics_timeout=120, time_out_unit="s"):

        params = AnalyticsParams.build()
        params = params.rawParam("pretty", pretty)
        params = params.rawParam("timeout",
                                 str(analytics_timeout) + time_out_unit)
        params = params.rawParam("username", username)
        params = params.rawParam("password", password)
        params = params.rawParam("clientContextID", client_context_id)
        if client_context_id:
            params = params.withContextId(client_context_id)

        output = {}
        q = AnalyticsQuery.simple(statement, params)
        try:
            if mode or "EXPLAIN" in statement:
                return CBAS_helper_rest.execute_statement_on_cbas(
                    self, statement, mode, pretty, timeout, client_context_id,
                    username, password)

            result = self.bucket.query(q,timeout,TimeUnit.SECONDS)

            output["status"] = result.status()
            output["metrics"] = str(result.info().asJsonObject())

            try:
                output["results"] = str(result.allRows())
            except:
                output["results"] = None

            output["errors"] = json.loads(str(result.errors()))

            if str(output['status']) == "fatal":
                self.log.error(output['errors'])
                msg = output['errors'][0]['msg']
                if "Job requirement" in  msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif str(output['status']) == "success":
                output["errors"] = None
                pass
            else:
                self.log.error("Analytics query %s failed status:{0},result:{1}"
                               .format(output["status"], result))
                raise Exception("Analytics Service API failed")

        except TimeoutException as e:
            self.log.info("Request TimeoutException from Java SDK %s" % str(e))
            raise Exception("Request TimeoutException")
        except RequestCancelledException as e:
            self.log.info("RequestCancelledException from Java SDK %s"
                          % str(e))
            raise Exception("Request RequestCancelledException")
        except RejectedExecutionException as e:
            self.log.info("Request RejectedExecutionException from Java SDK %s"
                          % str(e))
            raise Exception("Request Rejected")
        except CouchbaseException as e:
            self.log.info("CouchbaseException from Java SDK %s" % str(e))
            raise Exception("CouchbaseException")
        except RuntimeException as e:
            self.log.info("RuntimeException from Java SDK %s" % str(e))
            raise Exception("Request RuntimeException")
        return output

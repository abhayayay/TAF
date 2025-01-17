"""
Created on August 18, 2023

@author: Vipul Bhardwaj
"""

import copy
import base64
import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class CreateBucket(GetCluster):

    def setUp(self, nomenclature="Buckets_Create"):
        GetCluster.setUp(self, nomenclature)

        # Initialise bucket params (for future bucket creation).
        self.expected_result = {
            "name": self.prefix + nomenclature,
            "type": "couchbase",
            "storageBackend": "couchstore",
            "memoryAllocationInMb": 100,
            "bucketConflictResolution": "seqno",
            "durabilityLevel": "none",
            "replicas": 1,
            "flush": False,
            "timeToLiveInSeconds": 0,
            "evictionPolicy": "fullEviction",
            "priority": None, #will be changed once maxTTL feature is merged to main
            "stats": {
                "itemCount": None,
                "opsPerSecond": None,
                "diskUsedInMib": None,
                "memoryUsedInMib": None
            }
        }
        self.bucket_ids = list()

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        # Delete the buckets that were created.
        if self.delete_buckets(self.organisation_id, self.project_id,
                               self.cluster_id, self.bucket_ids):
            self.log.error("Error while deleting buckets.")
        self.log.info("Successfully deleted buckets.")

        super(CreateBucket, self).tearDown()

    def validate_bucket_api_response(self, expected_res, actual_res):
        for key in actual_res:
            if key not in expected_res:
                return False
            if isinstance(actual_res[key], dict):
                self.validate_bucket_api_response(
                    expected_res[key], actual_res[key])
            elif expected_res[key]:
                if expected_res[key] != actual_res[key]:
                    return False
        return True

    def test_api_path(self):
        testcases = [
            {
                "description": "Create valid bucket"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace buckets with bucket in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/bucket",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{"
                       "}/buckets/bucket",
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Create bucket but with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Create bucket but with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Create bucket but with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            self.expected_result['name'] = self.generate_random_string(
                special_characters=False)
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.bucket_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]

            self.log.info("Creating bucket.")
            result = self.capellaAPI.cluster_ops_apis.create_bucket(
                org, proj, clus, self.expected_result['name'],
                self.expected_result['type'],
                self.expected_result['storageBackend'],
                self.expected_result['memoryAllocationInMb'],
                self.expected_result['bucketConflictResolution'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'],
                self.expected_result['flush'],
                self.expected_result['timeToLiveInSeconds'],
                self.expected_result['priority'])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_bucket(
                    org, proj, clus, self.expected_result['name'],
                    self.expected_result['type'],
                    self.expected_result['storageBackend'],
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['bucketConflictResolution'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'],
                    self.expected_result['flush'],
                    self.expected_result['timeToLiveInSeconds'],
                    self.expected_result['priority'])
            if result.status_code == 201 and "expected_error" not in testcase:
                self.expected_result['id'] = result.json()['id']
                self.bucket_ids.append(result.json()['id'])
            else:
                self.validate_testcase(result, 201, testcase, failures)

            self.capellaAPI.cluster_ops_apis.bucket_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/buckets"

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.api_keys.update(
            self.create_api_keys_for_all_combinations_of_roles(
                [self.project_id]))

        resp = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, "Auth_Project")
        if resp.status_code == 201:
            other_project_id = resp.json()["id"]
        else:
            self.fail("Error while creating project")

        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"],
            }
            if not any(element in ["organizationOwner", "projectOwner",
                                   "projectManager"] for
                       element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        self.auth_test_extension(testcases, other_project_id)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            self.expected_result['name'] = self.generate_random_string(
                special_characters=False)

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id, self.cluster_id)
            self.log.debug("Cluster state healthy.")

            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_result['name'], self.expected_result['type'],
                self.expected_result['storageBackend'],
                self.expected_result['memoryAllocationInMb'],
                self.expected_result['bucketConflictResolution'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'],
                self.expected_result['flush'],
                self.expected_result['timeToLiveInSeconds'],
                self.expected_result['priority'], headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_bucket(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_result['name'], self.expected_result['type'],
                    self.expected_result['storageBackend'],
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['bucketConflictResolution'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'],
                    self.expected_result['flush'],
                    self.expected_result['timeToLiveInSeconds'],
                    self.expected_result['priority'], headers=header)
            if result.status_code == 201 and "expected_error" not in testcase:
                self.expected_result['id'] = result.json()['id']
                self.bucket_ids.append(result.json()['id'])

                if not self.validate_bucket_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 201, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful - No Errors.")
            else:
                self.validate_testcase(result, 201, testcase, failures)

            if len(self.bucket_ids) >= 30:
                self.log.warning("Bucket limit for cluster reached, flushing "
                                 "all current buckets.")
                self.delete_buckets(self.organisation_id, self.project_id,
                                    self.cluster_id, self.bucket_ids)

        self.update_auth_with_api_token(self.org_owner_key["token"])
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project {}"
                            .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(str(combination[0]),
                                                      str(combination[1]),
                                                      str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if combination[1] == "" or combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[2] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [int, bool, float, list, tuple, set,
                                      type(None)] for variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process "
                                   "the request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "message": "Access Denied.",
                        "httpStatusCode": 403
                    }
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                else:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}."
                        .format(combination[1], combination[2])
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            self.expected_result['name'] = self.generate_random_string(
                special_characters=False)
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id,
                        self.cluster_id)
            self.log.debug("Cluster state healthy.")

            result = self.capellaAPI.cluster_ops_apis.create_bucket(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], self.expected_result['name'],
                self.expected_result['type'],
                self.expected_result['storageBackend'],
                self.expected_result['memoryAllocationInMb'],
                self.expected_result['bucketConflictResolution'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'],
                self.expected_result['flush'],
                self.expected_result['timeToLiveInSeconds'],
                self.expected_result['priority'], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_bucket(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], self.expected_result['name'],
                    self.expected_result['type'],
                    self.expected_result['storageBackend'],
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['bucketConflictResolution'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'],
                    self.expected_result['flush'],
                    self.expected_result['timeToLiveInSeconds'],
                    self.expected_result['priority'], **kwarg)
            if result.status_code == 201 and "expected_error" not in testcase:
                self.expected_result['id'] = result.json()['id']
                self.bucket_ids.append(result.json()['id'])

                if not self.validate_bucket_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 201, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful - No Errors.")
            else:
                self.validate_testcase(result, 201, testcase, failures)

            if len(self.bucket_ids) >= 30:
                self.log.warning("Bucket limit for cluster reached, flushing "
                                 "all current buckets.")
                self.delete_buckets(self.organisation_id, self.project_id,
                                    self.cluster_id, self.bucket_ids)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()

        for key in self.expected_result:
            if key in ["stats", "evictionPolicy"]:
                continue

            values = [
                "", 1, 0, 100000, -1, 123.123,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(500, special_characters=False),
            ]
            for value in values:
                self.expected_result['name'] = self.generate_random_string(
                    special_characters=False)
                testcase = copy.deepcopy(self.expected_result)
                testcase[key] = value
                for param in ["stats", "evictionPolicy"]:
                    del testcase[param]

                testcase["description"] = "Testing `{}` with val: {} of {}"\
                                          .format(key, value, type(value))
                if (
                        (key in ["name", "type", "storageBackend",
                                 "bucketConflictResolution", "durabilityLevel"]
                            and not isinstance(value, str))
                        or (key in ["memoryAllocationInMb",
                                    "timeToLiveInSeconds", "replicas"]
                            and not isinstance(value, int))
                        or (key == "flush" and not isinstance(value, bool))
                        or (key == "priority" and not isinstance(value, int))
                ):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": 'Bad Request. Error: body contains '
                                   'incorrect JSON type for '
                                   'field "{}".'.format(key)
                    }
                elif (
                 (key == 'timeToLiveInSeconds' and (value >= 0)) or
                 (key == 'replicas' and value in range(0, 4)) or
                 (key == 'flush' and isinstance(value, bool)) or
                 (key == 'bucketConflictResolution'
                     and value in ['seqno', 'lww']) or
                 (key == 'memoryAllocationInMb' and isinstance(value, int)
                     and value >= 100) or
                 (key == 'durabilityLevel' and value in [
                            "none", "majority", "majorityAndPersistActive",
                            "persistToMajority"]) or
                 (key == 'name' and value != "" and len(value) <= 100) or
                 (key == 'type' and value in ["couchbase", "ephemeral", ""]) or
                 (key == 'storageBackend' and value in ["couchstore", "magma"])
                ):
                    continue
                elif key == "durabilityLevel":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 6005,
                        "hint": "The provided durability level is not "
                                "supported. The supported levels are 'none', "
                                "'majority', 'persistToMajority', and "
                                "'majorityAndPersistActive'. Please choose a "
                                "valid durability level for the bucket.",
                        "httpStatusCode": 422,
                        "message": "The durability level {} provided is not "
                                   "supported. The supported level are 'none',"
                                   " 'majority', 'persistToMajority', and "
                                   "'majorityAndPersistActive'.".format(value)
                    }
                elif key == "bucketConflictResolution":
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 6004,
                        "hint": "The provided conflict resolution is not "
                                "supported. Please choose a valid conflict "
                                "resolution strategy for the bucket.",
                        "httpStatusCode": 400,
                        "message": "Unsupported conflict resolution "
                                   "'{}'. Acceptable resolutions are "
                                   "Sequence Number (seqno) or Last Write "
                                   "Wins (lww).".format(value)
                    }
                elif key == "memoryAllocationInMb" and value < 100:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 6023,
                        "hint": "Cannot create bucket. The requested size of "
                                "the bucket is less than the minimum amount "
                                "of 100MB. Please choose a larger size for "
                                "the bucket.",
                        "httpStatusCode": 422,
                        "message": "Cannot create bucket. The requested size "
                                   "of the bucket is less than the minimum "
                                   "amount of 100MB."
                    }
                elif key == "replicas":
                    testcase["expected_status_code"] = 422
                    if value < 1:
                        testcase["expected_error"] = {
                            "code": 6026,
                            "hint": "The replica count provided for the "
                                    "bucket is not valid. The minimum number "
                                    "of replicas is 0. Please increase the "
                                    "number of replicas for the bucket.",
                            "httpStatusCode": 422,
                            "message": "The replica count provided for the "
                                       "buckets is not valid. The minimum "
                                       "number of replicas is (0)."
                        }
                    elif value > 3:
                        testcase["expected_error"] = {
                            "code": 6024,
                            "hint": "The replica count provided for the "
                                    "bucket is not valid. The maximum number "
                                    "of replicas is 3. Please reduce the "
                                    "number of replicas for the bucket.",
                            "httpStatusCode": 422,
                            "message": "The replica count provided for the "
                                       "buckets is not valid. The maximum "
                                       "number of replicas is (3)."
                        }
                elif key == "name":
                    testcase["expected_status_code"] = 422
                    if value == "":
                        testcase["expected_error"] = {
                            "code": 6011,
                            "hint": "The bucket name is not valid. A bucket "
                                    "name cannot be empty. Please provide a "
                                    "valid name for the bucket.",
                            "httpStatusCode": 422,
                            "message": "The bucket name is not valid. A bucket"
                                       " name can not be empty."
                        }
                    elif len(value) > 100:
                        testcase["expected_error"] = {
                            "code": 6013,
                            "hint": "The bucket name provided is not valid. "
                                    "The maximum length of the bucket name "
                                    "can be 100 characters. Please provide a "
                                    "valid name for the bucket within the "
                                    "allowed length.",
                            "httpStatusCode": 422,
                            "message": "The bucket name provided is not "
                                       "valid. The maximum length of the "
                                       "bucket name can be (100) characters."
                        }
                elif key == "type":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 6016,
                        "hint": "The provided bucket type is invalid. Bucket "
                                "type must be one of 'couchbase' or "
                                "'ephemeral'. Please provide a valid bucket "
                                "type for the bucket.",
                        "httpStatusCode": 422,
                        "message": "The provided bucket type of '{}' is "
                                   "invalid. Bucket type must be one of "
                                   "'couchbase' or 'ephemeral'.".format(value)
                    }
                elif key == "storageBackend":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 8020,
                        "hint": "Returned when attempting to create a "
                                "couchstore bucket and providing an invalid "
                                "storageBackend value. Use either "
                                "'couchstore' or 'magma'.",
                        "httpStatusCode": 422,
                        "message": "{} is an invalid configuration option "
                                   "for storageBackend. Use either "
                                   "'couchstore' or 'magma'".format(value)
                    }
                elif key == "timeToLiveInSeconds":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 8021,
                        "hint": "Returned when a time-to-live unit that is "
                                "not supported is given during bucket "
                                "creation. This should be a non-negative "
                                "value.",
                        "httpStatusCode": 422,
                        "message": "The time-to-live value provided is not "
                                   "supported. It should be a non-negative "
                                   "integer."
                    }
                elif key == "priority" and value not in range(0, 1000):
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "The bucket priority can be set to a "
                                   "value between 0 and 1000."
                    }
                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['description'])

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(
                self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id,
                    self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id,
                    self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(
                        int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id,
                        self.cluster_id)
            self.log.debug("Cluster state healthy.")

            result = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                testcase["name"], testcase["type"], testcase["storageBackend"],
                testcase["memoryAllocationInMb"],
                testcase["bucketConflictResolution"],
                testcase["durabilityLevel"], testcase['replicas'],
                testcase['flush'], testcase['timeToLiveInSeconds'],
                testcase['priority'])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_bucket(
                    self.organisation_id, self.project_id, self.cluster_id,
                    testcase["name"], testcase["type"],
                    testcase["storageBackend"],
                    testcase["memoryAllocationInMb"],
                    testcase["bucketConflictResolution"],
                    testcase["durabilityLevel"], testcase['replicas'],
                    testcase['flush'], testcase['timeToLiveInSeconds'],
                    testcase['priority'])
            if result.status_code == 201:
                self.expected_result['id'] = result.json()['id']
                self.bucket_ids.append(result.json()['id'])

                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    self.log.warning("Result: {}".format(result.content))
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful - No Errors.")
            else:
                self.validate_testcase(result, 201, testcase, failures)

            if len(self.bucket_ids) >= 30:
                self.log.warning("Bucket limit for cluster reached, flushing "
                                 "all current buckets.")
                self.delete_buckets(self.organisation_id, self.project_id,
                                    self.cluster_id, self.bucket_ids)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_bucket,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.generate_random_string(special_characters=False),
             self.expected_result['type'],
             self.expected_result['storageBackend'],
             self.expected_result['memoryAllocationInMb'],
             self.expected_result['bucketConflictResolution'],
             self.expected_result['durabilityLevel'],
             self.expected_result['replicas'], self.expected_result['flush'],
             self.expected_result['timeToLiveInSeconds'],
             self.expected_result['priority'])
        ]]
        for i in range(self.input.param("num_api_keys", 1)):
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id,
                self.generate_random_string(prefix=self.prefix),
                ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    self.organisation_id,
                    self.generate_random_string(prefix=self.prefix),
                    ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 201:
                self.api_keys["organizationOwner_{}".format(i)] = resp.json()
            else:
                self.fail("Error while creating API key for "
                          "organizationOwner_{}".format(i))

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # invalid name param, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_bucket,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.generate_random_string(special_characters=False),
             self.expected_result['type'],
             self.expected_result['storageBackend'],
             self.expected_result['memoryAllocationInMb'],
             self.expected_result['bucketConflictResolution'],
             self.expected_result['durabilityLevel'],
             self.expected_result['replicas'], self.expected_result['flush'],
             self.expected_result['timeToLiveInSeconds'],
             self.expected_result['priority'])
        ]]
        org_roles = self.input.param("org_roles", "organizationOwner")
        proj_roles = self.input.param("proj_roles", "projectDataReader")
        org_roles = org_roles.split(":")
        proj_roles = proj_roles.split(":")

        api_key_dict = self.create_api_keys_for_all_combinations_of_roles(
            [self.project_id], proj_roles, org_roles)
        for i, api_key in enumerate(api_key_dict):
            if api_key in self.api_keys:
                self.api_keys["{}_{}".format(api_key_dict[api_key], i)] = \
                    api_key_dict[api_key]
            else:
                self.api_keys[api_key] = api_key_dict[api_key]

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # invalid name param, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]
            #   # 400 response.
            if "400" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["400"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

"""
Created on August 02, 2023

@author: Vipul Bhardwaj
"""

import time
import base64
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class ListCluster(GetProject):

    def setUp(self, nomenclature="Clusters_List"):
        GetProject.setUp(self, nomenclature)

        self.cluster_name = self.prefix + nomenclature
        self.expected_result = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": [
                {
                    "name": self.cluster_name,
                    "description": None,
                    "cloudProvider": {
                        "type": "aws",
                        "region": "us-east-1",
                        "cidr": CapellaUtils.get_next_cidr() + "/20"
                    },
                    "couchbaseServer": {
                        "version": str(self.input.param("server_version", 7.2))
                    },
                    "serviceGroups": [
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 16
                                },
                                "disk": {
                                    "storage": 50,
                                    "type": "gp3",
                                    "iops": 3000,
                                    "autoExpansion": "on"
                                }
                            },
                            "numOfNodes": 3,
                            "services": [
                                "data"
                            ]
                        }
                    ],
                    "availability": {
                        "type": "single"
                    },
                    "support": {
                        "plan": "basic",
                        "timezone": "GMT"
                    },
                    "currentState": None,
                    "audit": {
                        "createdBy": None,
                        "createdAt": None,
                        "modifiedBy": None,
                        "modifiedAt": None,
                        "version": None

                    },
                    "connectionString": None,
                    "configurationType": None
                }
            ]
        }

        result = self.select_CIDR(
            self.organisation_id, self.project_id, self.cluster_name,
            self.expected_result["data"][0]['cloudProvider'],
            self.expected_result["data"][0]['couchbaseServer'],
            self.expected_result["data"][0]['serviceGroups'],
            self.expected_result["data"][0]['availability'],
            self.expected_result["data"][0]['support'])
        if result.status_code != 202:
            self.log.error("Failed while deploying cluster")
            self.tearDown()
            self.fail("!!!...CIDR selection failed...!!!")

        self.cluster_id = result.json()["id"]
        self.log.info("Waiting for cluster {} to be deployed."
                      .format(self.cluster_id))
        if not self.wait_for_deployment(self.project_id, self.cluster_id):
            self.tearDown()
            self.fail("!!!...Cluster deployment failed...!!!")
        self.log.info("Successfully deployed Cluster.")

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        # Delete the cluster that was created.
        self.log.info("Destroying Cluster: {}".format(self.cluster_id))
        if self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, self.project_id,
                self.cluster_id).status_code != 202:
            self.log.error("Error while deleting cluster.")

        # Wait for the cluster to be destroyed.
        self.log.info("Waiting for cluster to be destroyed.")
        if not self.verify_project_empty(self.project_id):
            self.fail("Cluster could not be destroyed")
        self.log.info("Cluster destroyed successfully.")

        super(ListCluster, self).tearDown()

    def validate_cluster_api_response(self, expected_res, actual_res):
        for key in actual_res:
            if key not in expected_res:
                return False
            if isinstance(expected_res[key], dict):
                self.validate_cluster_api_response(
                    expected_res[key], actual_res[key])
            elif isinstance(expected_res[key], list):
                if key == "services":
                    for service in expected_res[key]:
                        if service not in actual_res[key]:
                            return False
                    continue
                for i in range(len(expected_res[key])):
                    if (actual_res[key] is "data" and
                            actual_res[key][i]["name"] != self.cluster_name):
                        continue
                    self.validate_cluster_api_response(
                        expected_res[key][i], actual_res[key][i])
            elif expected_res[key]:
                if expected_res[key] == "version":
                    if expected_res[key] not in actual_res[key]:
                        return False
                elif expected_res[key] != actual_res[key]:
                    return False
        return True

    def test_api_path(self):
        testcases = [
            {
                "description": "List clusters with valid path, params."
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace clusters with cluster in URI",
                "url": "/v4/organizations/{}/projects/{}/cluster",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/cluster",
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
                "description": "Fetch cluster but with non-hex organizationID",
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
                "description": "Fetch cluster but with non-hex projectID",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]

            result = self.capellaAPI.cluster_ops_apis.list_clusters(org, proj)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    org, proj)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_cluster_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 200, testcase, failures)

            self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters"

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
                "token": self.api_keys[role]["token"]
            }
            if not any(element in ["organizationOwner", "projectDataReader",
                                   "projectOwner", "projectDataReaderWriter",
                                   "projectViewer", "projectManager"] for
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
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, self.project_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    self.organisation_id, self.project_id, headers=header)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_cluster_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 200, testcase, failures)

        self.update_auth_with_api_token(self.org_owner_key["token"])
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            self.log.error("Error while deleting project {}"
                           .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}"
                       .format(self.organisation_id, self.project_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}".format(
                    str(combination[0]), str(combination[1])),
                "organizationID": combination[0],
                "projectID": combination[1]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id):
                if combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[1] == "" or any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [type(combination[0]),
                                      type(combination[1])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process the "
                                   "request due to something that is "
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
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 2000,
                        "hint": "Check if the project ID is valid.",
                        "httpStatusCode": 404,
                        "message": "The server cannot find a project by its "
                                   "ID."
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.list_clusters(
                testcase["organizationID"], testcase["projectID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    testcase["organizationID"], testcase["projectID"], **kwarg)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_cluster_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 200, testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.list_clusters,
                          (self.organisation_id, self.project_id)]]

        for i in range(self.input.param("num_api_keys", 1)):
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, self.generate_random_string(),
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
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.list_clusters,
                          (self.organisation_id, self.project_id)]]

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
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

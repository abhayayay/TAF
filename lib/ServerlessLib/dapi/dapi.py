import logging
import requests
import base64

log = logging.getLogger()


class RestfulDAPI:
    def __init__(self, args):
        self.dapi_endpoint = "https://" + args.get("dapi_endpoint")
        self.username = args.get("access_token")
        self.password = args.get("access_secret")
        self.header = self._create_headers(self.username, self.password)
        self._log = logging.getLogger(__name__)

    def _create_headers(self, username=None, password=None, contentType='application/json', connection='close'):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        return {'Content-Type': contentType,
                'Authorization': 'Basic %s' % authorization,
                'Connection': connection,
                'Accept': '*/*'}

    def _urllib_request(self, api, method='GET', headers=None,
                        params='', timeout=300, verify=False):
        session = requests.Session()
        headers = headers or self.header
        try:
            if method == "GET":
                resp = session.get(api, params=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "POST":
                resp = session.post(api, data=params, headers=headers,
                                    timeout=timeout, verify=verify)
            elif method == "DELETE":
                resp = session.delete(api, data=params, headers=headers,
                                      timeout=timeout, verify=verify)
            elif method == "PUT":
                resp = session.put(api, data=params, headers=headers,
                                   timeout=timeout, verify=verify)
            return resp
        except requests.exceptions.HTTPError as errh:
            self._log.error("HTTP Error {0}".format(errh))
        except requests.exceptions.ConnectionError as errc:
            self._log.error("Error Connecting {0}".format(errc))
        except requests.exceptions.Timeout as errt:
            self._log.error("Timeout Error: {0}".format(errt))
        except requests.exceptions.RequestException as err:
            self._log.error("Something else: {0}".format(err))

    def check_dapi_health(self):
        url = self.dapi_endpoint + "/health"
        return self._urllib_request(url)

    def get_doc(self, doc_id, scope, collection):
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
              + collection + "/docs/" + doc_id
        return self._urllib_request(url)

    def insert_doc(self, doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + doc_id
        return self._urllib_request(url, methos="POST", params=params)

    def upsert_doc(self, existing_doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + existing_doc_id + "&upsert=true"
        return self._urllib_request(url, methos="POST", params=params)

    def update_doc(self, existing_doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + existing_doc_id
        return self._urllib_request(url, methos="POST", params=params)

    def delete_doc(self):
        pass

    def get_collection_docs(self):
        pass

    def create_primary_index(self):
        pass

    def execute_query(self):
        pass
import requests
import json
import base64


class MetaDataService:
    def __init__(self, user, password, env, port=8090):
        self.base_url = f'http://{env}:{port}/security/1.0'
        self.base_decode = f"{user}:{password}"
        self.basic = base64.b64encode(self.base_decode.encode("ascii")).decode("ascii")
        self.bearer_token = self.authenticate()
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "content-type": "application/json",
        }
        self.cluster_id = self.get_cluster_id()
        self.cluster_data = {"clusters": {"kafka-cluster": self.cluster_id}}

    def authenticate(self):
        headers = {
            "Authorization": f"Basic {self.basic}"
        }
        try:
            r = requests.get(f"{self.base_url}/authenticate", headers=headers)
            return json.loads(r.content)['auth_token']
        except json.decoder.JSONDecodeError:
            return "authentication issue"

    def get_cluster_id(self):
        try:
            r = requests.get(f"{self.base_url}/metadataClusterId", headers=self.headers)
            return r.content.decode('ascii')
        except:
            return None

    def get_rolenames(self):
        try:
            # print(self.headers)
            r = requests.get(f"{self.base_url}/roleNames", headers=self.headers)
            return json.loads(r.content)
        except json.decoder.JSONDecodeError:
            return None

    def list_rolebinding_resources(self, principal, rolename):
        role_names = self.get_rolenames()
        if role_names and rolename not in role_names:
            return "Invalid roleName"

        r = requests.post(f"{self.base_url}/principals/{principal}/roles/{rolename}/resources", headers=self.headers, data=json.dumps(self.cluster_data))
        resource = json.loads(r.content)
        if not len(resource):
            return "either User/Group or role bindings  does not exists"
        return resource

    def create_rolebinding_resources(self, principal, rolename , resource, pattern_type='LITERAL'):
        pattern_types = ['PREFIXED', 'CONFLUENT_ONLY_TENANT_MATCH', 'MATCH', 'CONFLUENT_ALL_TENANT_ANY',
                         'CONFLUENT_ALL_TENANT_PREFIXED', 'CONFLUENT_ALL_TENANT_LITERAL', 'UNKNOWN', 'ANY', 'LITERAL']
        if pattern_type not in pattern_types:
            return "Invalid pattern type"
        role_names = self.get_rolenames()
        if role_names and rolename not in role_names:
            return "Invalid roleName"

        resource_type, name = resource.split(":")
        data = {"scope": self.cluster_data,
                "resourcePatterns": [
                    {
                        "resourceType": resource_type,
                        "name": name,
                        "patternType": pattern_type
                    }]
                }

        # print(data)
        r = requests.post(f"{self.base_url}/principals/{principal}/roles/{rolename}/bindings", headers=self.headers,
                          data=json.dumps(data))
        if r.status_code == 204:
            return "role binding created"
        else:
            return r.content


if __name__ == "__main__":
    mds = MetaDataService('xx', 'xx', "c-kafka-qa4.copart.com")
    # mds.authenticate()
    # print(mds.bearer_token)
    # print(mds.get_clusterid())
    #print(mds.cluster_id)
    #print(mds.get_roleNames())
    #print(mds.list_rolebinding_resources("User:test-qa-consumer", "DeveloperRead"))
    print(mds.create_rolebinding_resources("User:test-qa-consumer", "DeveloperRead", 'Topic:testktable', "PREFIXED"))



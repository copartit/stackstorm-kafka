from rbac.mds import MetaDataService

from lib.base_admin_action import BaseAdminAction


class CreateRoleBindingAction(BaseAdminAction):
    def run(
        self,
        kafka_broker,
        mds_port,
        principal,
        role,
        resource,
        pattern_type,
        mds_username,
        mds_password,
    ):
        mds = MetaDataService(mds_username, mds_password, kafka_broker, port=mds_port)
        return mds.create_rolebinding_resources(principal, role, resource, pattern_type)


if __name__ == "__main__":
    import os

    KAFKA_BROKER = os.environ.get("KAFKA_BROKER",'c-kafka-qa4-rn.copart.com')
    MDS_PORT = os.environ.get("MDS_PORT", 8090)
    MDS_USERNAME = os.environ.get("MDS_USERNAME")
    MDS_PASSWORD = os.environ.get("MDS_PASSWORD")
    PRINCIPAL = os.environ.get("PRINCIPAL", "User:test-qa-consumer")
    RESOURCE = os.environ.get("RESOURCE", "Topic:testktable")
    ROLE = os.environ.get("ROLE", "DeveloperRead")
    PATTERN_TYPE = os.environ.get("PATTERN_TYPE", "LITERAL")

    action = CreateRoleBindingAction()

    res = action.run(
        kafka_broker=KAFKA_BROKER,
        mds_port=MDS_PORT,
        principal=PRINCIPAL,
        role=ROLE,
        resource=RESOURCE,
        pattern_type=PATTERN_TYPE,
        mds_username=MDS_USERNAME,
        mds_password=MDS_PASSWORD
    )
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(res)

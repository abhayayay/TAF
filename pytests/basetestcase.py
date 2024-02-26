from TestInput import TestInputSingleton

runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "dedicated":
    from dedicatedBase.dedicatedbasetestcase import OnCloudBaseTest as CbBaseTest
    from dedicatedBase.dedicatedbasetestcase import ClusterSetup as CbClusterSetup
elif runtype == "serverless":
    from serverlessBase.serverlessbasetestcase import OnCloudBaseTest as CbBaseTest
    from serverlessBase.serverlessbasetestcase import ClusterSetup as CbClusterSetup
elif runtype == "columnar":
    from columnarBase.columnarbasetestcase import ColumnarBaseTest as CbBaseTest
    from columnarBase.columnarbasetestcase import ClusterSetup as CbClusterSetup
else:
    from OnPremBase.onPrem_basetestcase import OnPremBaseTest as CbBaseTest
    from OnPremBase.onPrem_basetestcase import ClusterSetup as CbClusterSetup

class BaseTestCase(CbBaseTest):
    pass


class ClusterSetup(CbClusterSetup):
    pass

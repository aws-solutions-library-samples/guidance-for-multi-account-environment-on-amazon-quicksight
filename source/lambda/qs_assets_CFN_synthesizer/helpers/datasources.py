from enum import Enum

class SourceType(Enum):
    S3 = 1
    ATHENA = 2    
    MYSQL = 3
    ORACLE = 4
    POSTGRESQL = 5
    MARIADB = 6
    AURORA = 7
    AURORA_POSTGRESQL = 8
    SQLSERVER = 9
    REDSHIFT = 10
    RDS = 11

class QSDataSourceDef:
    name =''
    id = ''
    datasetArns = []
    CFNId = ''
    index = 0
    def __init__(self, name: str, id: str, index: int):
        self.name = name
        self.id = id
        self.CFNId = 'DS{id}'.format(id=id.replace('-', ''))
        self.index = index

class QSServiceDatasourceDef(QSDataSourceDef):
    parameters = {}
    type = None

    def __init__(self, name: str, id: str, parameters: object, type: SourceType, index: int):
        self.type = type
        self.parameters = parameters

        if not isinstance(type, SourceType):
            raise TypeError("resources.datasources.QSServiceDatasourceDef Error: Type must be an instance of SourceType enum")

        if type == SourceType.S3:
            if 'Bucket' not in parameters or 'Key' not in parameters:
                raise ValueError("resources.datasources.QSServiceDatasourceDef Error: S3 Datasource Type should contain Bucket and Key in properties")
        
        if type == SourceType.ATHENA:
            if 'WorkGroup' not in parameters:
                raise ValueError("resources.datasources.QSServiceDatasourceDef Error: Athena Datasource Type should contain WorkGroup in properties")        
        

        super().__init__(name, id, index)


class QSRDSDatasourceDef(QSDataSourceDef):
    vpcConnectionArn = ''
    instanceId = ''
    database = ''
    type = SourceType.RDS
    vpcConnectionArn = ''

    def __init__(self, name: str, id: str, parameters: object, vpcConnectionArn: str, index: int):
        if vpcConnectionArn is not None:
            self.vpcConnectionArn = vpcConnectionArn
        if 'InstanceId' in parameters:
            self.instanceId = parameters['InstanceId']
        if 'Database' in parameters:
            self.database = parameters['Database']

        super().__init__(name, id, index)
        

class QSRDBMSDatasourceDef(QSDataSourceDef):

    host = ''
    port = 0    
    database = ''
    vpcConnectionArn = ''
    clusterId = ''
    parameters = {}

    def __init__(self, name: str, id: str, parameters: object, vpcConnectionArn: str, index: int):
        if vpcConnectionArn is not None:
            self.vpcConnectionArn = vpcConnectionArn
        if 'Host' in parameters:
            self.host = parameters['Host']
        if 'Database' in parameters:
            self.database = parameters['Database']
        if 'Port' in parameters:
            self.port = parameters['Port']
        if 'ClusterId' in parameters:
            self.clusterId = parameters['ClusterId']
        self.parameters = parameters

        super().__init__(name, id, index)
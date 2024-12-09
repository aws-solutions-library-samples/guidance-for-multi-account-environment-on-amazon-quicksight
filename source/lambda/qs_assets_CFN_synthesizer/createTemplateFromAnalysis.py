import logging
import yaml
import json
import os
import time
import copy
from zipfile import ZipFile
import boto3
from botocore.exceptions import ClientError
from helpers.datasets import QSDataSetDef
from helpers.analysis import QSAnalysisDef
from helpers.datasources import SourceType, QSDataSourceDef, QSServiceDatasourceDef, QSRDSDatasourceDef, QSRDBMSDatasourceDef
from helpers.datasets import ImportMode
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.tz import tz
from urllib.request import urlretrieve

utc = tz.gettz('UTC')
utc_now = datetime.now(tz=utc)

FIRST_STAGE_ACCOUNT_ID = os.environ['SOURCE_AWS_ACCOUNT_ID']
DEPLOYMENT_ACCOUNT_ID = os.environ['DEPLOYMENT_ACCOUNT_ID']
AWS_REGION = os.environ['AWS_REGION']
DEPLOYMENT_S3_BUCKET = os.environ['DEPLOYMENT_S3_BUCKET']
DEPLOYMENT_S3_REGION = os.environ['DEPLOYMENT_S3_REGION']
ASSUME_ROLE_EXT_ID = os.environ['ASSUME_ROLE_EXT_ID']
ANALYSIS_ID = ''
STAGES_NAMES = os.environ['STAGES_NAMES']
REPLICATION_METHOD = os.environ['REPLICATION_METHOD']
GENERATE_NESTED_STACKS = os.environ['GENERATE_NESTED_STACKS']
REMAP_DS = os.environ['REMAP_DS']
PIPELINE_NAME = os.environ['PIPELINE_NAME'] if 'PIPELINE_NAME' in os.environ else ''
PARAMETER_DEFINITION_TABLE_NAME = 'QSAssetParameters-{pipelineName}'.format(pipelineName=PIPELINE_NAME)
MODE = os.environ['MODE'] if 'MODE' in os.environ else 'INITIALIZE' 
CONFIGURATION_FILES_PREFIX = '{pipeline_name}/ConfigFiles'.format(pipeline_name=PIPELINE_NAME)
ASSETS_FILES_PREFIX = '{pipeline_name}/CFNTemplates'.format(pipeline_name=PIPELINE_NAME)
PARAMETER_DEFINITION_TABLE_NAME = 'QSAssetParameters-{pipelineName}'.format(pipelineName=PIPELINE_NAME)
TRACKED_ASSETS_TABLE_NAME = 'QSTrackedAssets-{pipelineName}'.format(pipelineName=PIPELINE_NAME)
CONFIGURATION_FILES_PREFIX = '{pipeline_name}/ConfigFiles'.format(pipeline_name=PIPELINE_NAME)
ASSETS_FILES_PREFIX = '{pipeline_name}/CFNTemplates'.format(pipeline_name=PIPELINE_NAME)


DEPLOYMENT_DEV_ACCOUNT_ROLE_ARN = 'arn:aws:iam::{deployment_account_id}:role/DevAccountS3AccessRole-QSCICD-{pipeline_name}'.format(deployment_account_id=DEPLOYMENT_ACCOUNT_ID, pipeline_name=PIPELINE_NAME)
OUTPUT_DIR = '/tmp/output/'


try:
    os.mkdir(OUTPUT_DIR)
except FileExistsError:
    print('Output dir {output_dir} already exists, skipping'.format(output_dir=OUTPUT_DIR))


qs = boto3.client('quicksight', region_name=AWS_REGION)


def generateQSTemplateCFN(analysisDefObj:QSAnalysisDef, appendContent:dict):
    """Function that generates a Cloudformation AWS::QuickSight::Template resource https://a.co/7A8bfh7
    synthesized from a given analysisName

    Parameters:
    analysisDefObj (QSAnalysisDef): Analysis name that will be templated   
    appendContent(dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements

    Returns:
    dict: appendContent Object that represents the synthesized CFN template 

    Example:
    >>> generateQSTemplateCFN('Analysis Name', {'Dataset1': 'DatasetPlaceholder1', 'Dataset2': 'DatasetPlaceholder2'}, 'Analysis ARN')

    """
   
    template_version = 'QS_CI_CD_TEMPLATE_ANALYSIS_{analysis_id}_{suffix}'.format(suffix=utc_now.strftime('%d-%m-%y-%H-%M-%S'), analysis_id=analysisDefObj.id)

    if appendContent is None:
        print("Append content is None")
        raise ValueError("Error in createTemplateFromAnalysis:generateQSTemplateCFN, Append content is None")

    with open('resources/template_resource_CFN_skel.yaml', 'r') as file:
        yaml_template = yaml.safe_load(file)

    template_properties = yaml_template['Properties']
    analysis_id = analysisDefObj.id

    # properties in template

    templateId = analysisDefObj.TemplateId
    templateCFNResourceId = 'Template{analysis_cfn_id}'.format(analysis_cfn_id=analysisDefObj.CFNId)

    template_properties['SourceEntity']['SourceAnalysis']['Arn']['Fn::Sub'] = template_properties['SourceEntity']['SourceAnalysis']['Arn']['Fn::Sub'].replace('{analysis_id}', analysis_id)
    template_properties['TemplateId'] = templateId
    template_properties['Name'] = 'CI CD Template for analysis {name}'.format(name=analysisDefObj.name)
    template_properties['VersionDescription'] = template_version

    # set up dataset references


    dataset_ref_list = []

    for datasetObj in analysisDefObj.datasets:
        if datasetObj.isRLS:
            # It is a RLS dataset no need to include it on template
            continue
        dataset = {}
        datasetArnSubStr = 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:dataset/{dataset_id}'.replace('{dataset_id}', datasetObj.id)
        dataset['DataSetArn'] = {}
        dataset['DataSetArn']['Fn::Sub'] = datasetArnSubStr
        dataset['DataSetPlaceholder'] = datasetObj.placeholdername
        dataset_ref_list.append(dataset)

    template_properties['SourceEntity']['SourceAnalysis']['DataSetReferences'] = dataset_ref_list

    appendContent['Resources'][templateCFNResourceId] = yaml_template

    return appendContent

def generateDataSourceObject(datasourceId:str, datasourceIndex:int):
    
    QSSERVICE_DS = [SourceType.ATHENA.name, SourceType.S3.name]
    RDMBS_DS = [SourceType.AURORA.name, SourceType.AURORA_POSTGRESQL.name,SourceType.MYSQL.name,SourceType.MARIADB.name,SourceType.ORACLE.name,SourceType.SQLSERVER.name, SourceType.REDSHIFT.name]
    

    dataSourceDefObj = {}
    DSparameters ={}
    ret = qs.describe_data_source(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSourceId=datasourceId)
    dsType = ret['DataSource']['Type']
    datasourceName = ret['DataSource']['Name']
    datasourceArn = ret['DataSource']['Arn']
    
    if dsType in QSSERVICE_DS:

        if dsType == SourceType.S3.name:
            if 'DataSourceParameters' in ret['DataSource']:
                DSparameters['Bucket'] = ret['DataSource']['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Bucket']
                DSparameters['Key'] = ret['DataSource']['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Key']
            else:
                raise ValueError("Error in createTemplateFromAnalysis:generateDataSourceObject, S3 datasource {datasource_name} (datasource:{datasource_id}, type {type}) with index {index} has no DataSourceParameters. S3 datasources need to use a manifest file stored in S3). Cannot proceed further. Consider using REMAP_DS: ''TRUE''".format(datasource_name=datasourceName, index=datasourceIndex, type=dsType, datasource_id=datasourceId))
        
        if dsType == SourceType.ATHENA.name:
            DSparameters['WorkGroup'] = ret['DataSource']['DataSourceParameters']['AthenaParameters']['WorkGroup']
        
        dataSourceDefObj =  QSServiceDatasourceDef(name=datasourceName, arn=datasourceArn, parameters=DSparameters, type=SourceType[dsType], index=datasourceIndex)
        
    if dsType in RDMBS_DS:
        if 'SecretArn' not in ret['DataSource']:
            raise ValueError("Datasource {datasource_name} (datasource:{datasource_id}) is a {type} datasource and it is not configured with a secret, cannot proceed".format(type=dsType, datasource_name=datasourceName, datasource_id=datasourceId))
        
        DSparameters['SecretArn'] = ret['DataSource']['SecretArn']

        if 'VpcConnectionProperties' in ret['DataSource']:
            DSparameters['VpcConnectionArn'] = ret['DataSource']['VpcConnectionProperties']['VpcConnectionArn']

        if 'RdsParameters' in ret['DataSource']['DataSourceParameters']:
            #its an RDS datasource
            DSparameters['InstanceId'] = ret['DataSource']['DataSourceParameters']['RdsParameters']['InstanceId']
            DSparameters['Database'] = ret['DataSource']['DataSourceParameters']['RdsParameters']['Database']
            DSparameters['Type'] = SourceType[dsType].name
            dataSourceDefObj =  QSRDSDatasourceDef(name=datasourceName, arn=datasourceArn, parameters=DSparameters, type=SourceType[dsType],  index=datasourceIndex)
        else:
            datasourceParametersKey = list(ret['DataSource']['DataSourceParameters'].keys()).pop()
            DSparameters['Host'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Host']
            DSparameters['Port'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Port']
            DSparameters['Database'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Database']
            if dsType == SourceType.REDSHIFT.name:
                DSparameters['ClusterId'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['ClusterId']
            dataSourceDefObj =  QSRDBMSDatasourceDef(name=datasourceName, arn=datasourceArn, parameters=DSparameters, type=SourceType[dsType], index=datasourceIndex, dSourceParamKey=datasourceParametersKey)
     
    return dataSourceDefObj

        
def generateDataSourceCFN(datasourceDefObj: QSDataSourceDef, appendContent:dict, remap:bool):
    """
    Function that generates a Cloudformation AWS::QuickSight::DataSource resource https://a.co/2xRL70Q
    synthesized from the source environment account

    Parameters:
    datasourceDefObj (QSDataSourceDef): Datasource definition object encapsulating info of the datasource to create
    appendContent (dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements    
    remap (bool): Whether or not the datasource connection parameters (host, port, Athena workgroup ...) should be remapped

    
    Returns:
    dict: appendContent Object that represents the synthesized CFN template 
    object: dataSourceDefObj helper datasource object representing the datasource object

   
    """

    originalAppendContent = copy.deepcopy(appendContent)
    
    datasourceName = datasourceDefObj.name
    properties = {}
    RDMBS_DS = [SourceType.AURORA.name, SourceType.AURORA_POSTGRESQL.name,SourceType.MYSQL.name,SourceType.MARIADB.name,SourceType.ORACLE.name,SourceType.SQLSERVER.name, SourceType.REDSHIFT.name, SourceType.RDS.name]
    
    
    if appendContent is None:
        print("Append content is None")
        raise ValueError("Error in createTemplateFromAnalysis:generateDataSourceCFN, Append content is None")
                
    with open('resources/datasource_resource_CFN_skel.yaml', 'r') as file:
        yaml_datasource = yaml.safe_load(file)        
    
    datasourceIdKey = datasourceDefObj.CFNId
    index = datasourceDefObj.index
    appendContent['Resources'][datasourceIdKey] = yaml_datasource
    properties = appendContent['Resources'][datasourceIdKey]['Properties']  

    if datasourceIdKey in originalAppendContent['Resources']:
        print('Datasource with CFNId {cfn_id} already exists, skipping'.format(cfn_id=datasourceIdKey)) 
        return originalAppendContent

    properties['DataSourceId'] = datasourceDefObj.id    
    properties['Name'] = datasourceDefObj.name
    

    dsType = datasourceDefObj.type
    
    print("Processing datasource {datasource_name} (datasource:{datasource_id}, type {type})".format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType))
    
    if dsType == SourceType.S3:
        destBucketKey = '{cfnid}{type}DestinationBucket'.format(cfnid=datasourceIdKey, type=dsType.name)
        destKeyKey = '{cfnid}{type}DestinationKey'.format(cfnid=datasourceIdKey, type=dsType.name)
        templateS3Parameters = {
                'S3Parameters': {
                    'ManifestFileLocation': {}
                }
        } 
        if remap:
            appendContent['Parameters'].update({
                destBucketKey: {
                    'Description' : 'S3 bucket to use for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                    .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType),
                    'Type': 'String',
                    'Default': datasourceDefObj.parameters['Bucket']
                },
                destKeyKey: {
                    'Description' : 'S3 key to use for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                    .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType),
                    'Type': 'String',
                    'Default': datasourceDefObj.parameters['Key']
                }
            })
            templateS3Parameters['S3Parameters']['ManifestFileLocation']['Bucket'] = {
                'Ref': destBucketKey
            }
            templateS3Parameters['S3Parameters']['ManifestFileLocation']['Key'] = {
                'Ref': destKeyKey
            }
                    
        else:              
            templateS3Parameters['S3Parameters']['ManifestFileLocation']['Bucket'] = datasourceDefObj.parameters['Bucket']
            templateS3Parameters['S3Parameters']['ManifestFileLocation']['Key'] = datasourceDefObj.parameters['Key']
            
        properties['Type'] = dsType.name
        properties['DataSourceParameters'] = templateS3Parameters
    
    if dsType == SourceType.ATHENA:
        templateAthenaParameters = {
            'AthenaParameters': {}
        }
        if remap:
            athenaWorkgroupKey = '{cfnid}{type}Workgroup'.format(cfnid=datasourceIdKey, type=dsType.name)
            appendContent['Parameters'].update({
                athenaWorkgroupKey: {
                    'Description' : 'Athena Workgroup to use for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, \
                                        to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline.\
                                              This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                                              .format(datasource_name=datasourceName,  datasource_id=datasourceDefObj.id, type=dsType.name),
                    'Type': 'String',
                    'Default': datasourceDefObj.parameters['WorkGroup']
                }
            })
            templateAthenaParameters['AthenaParameters']['WorkGroup'] = {
                'Ref': athenaWorkgroupKey
            }
        else:
            templateAthenaParameters = {
                'AthenaParameters': {}
            }
            templateAthenaParameters['AthenaParameters']['WorkGroup'] = datasourceDefObj.parameters['WorkGroup']
        
        properties['Type'] = dsType.name
        properties['DataSourceParameters'] = templateAthenaParameters

    if dsType.name in RDMBS_DS:
        dsSecretKey = '{cfnid}SecretArn'.format(cfnid=datasourceIdKey)
        properties['Credentials'] = {
            'SecretArn':  {
                'Ref': dsSecretKey
            }
        }
        appendContent['Parameters'].update({
            dsSecretKey: {
                'Description' : 'Secret Arn to use for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN'
                .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                'Type': 'String'                
            }
        })
        if datasourceDefObj.vpcConnectionArn != '':
            vpcConnectionKey = '{cfnid}VpcConnectionArn'.format(cfnid=datasourceIdKey)
            properties['VpcConnectionProperties'] = {
                'VpcConnectionArn': {
                    'Ref': vpcConnectionKey
                }
            }
            appendContent['Parameters'].update({
                vpcConnectionKey:  {
                        'Description' : 'VPC Connection Arn to use for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String'
                    }
            }
            )

        if isinstance(datasourceDefObj, QSRDSDatasourceDef):
            #its an RDS datasource
            rdsInstanceParam = '{cfnid}RDSInstanceID'.format(cfnid=datasourceIdKey)
            databaseParam = '{cfnid}RDSDBName'.format(cfnid=datasourceIdKey)
            templateDSParameters = {
                'RdsParameters' : {
                }
            }
            if remap:
                appendContent['Parameters'].update({
                    rdsInstanceParam: {
                        'Description' : 'RDS Instance Id for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String',
                        'Default': datasourceDefObj.parameters['InstanceId']
                    },
                    databaseParam: {
                        'Description' : 'Database name for datasource {datasource_name} (datasource:{datasource_id}, type {type}) in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String',
                        'Default': datasourceDefObj.parameters['Database']
                    }
                })
                templateDSParameters['RdsParameters']['InstanceId'] = {
                    'Ref': rdsInstanceParam
                }
                templateDSParameters['RdsParameters']['Database'] = {
                    'Ref': databaseParam
                }

            else:
                templateDSParameters['RdsParameters']['InstanceId'] = datasourceDefObj.instanceId
                templateDSParameters['RdsParameters']['Database'] = datasourceDefObj.database
        else:
            #RDBMS connection
            datasourceParametersKey = datasourceDefObj.dSourceParamKey
            templateDSParameters = {
                datasourceParametersKey : {
                }
            } 
            if remap:
                databaseParam = '{cfnid}{type}DBName'.format(cfnid=datasourceIdKey, type=dsType.name)
                portParam = '{cfnid}{type}Port'.format(cfnid=datasourceIdKey, type=dsType.name)
                hostParam = '{cfnid}{type}Host'.format(cfnid=datasourceIdKey,type=dsType.name)
                appendContent['Parameters'].update({             
                    databaseParam: {
                        'Description' : 'Database name for datasource {datasource_name} (datasource:{datasource_id}, type {type}) to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String',
                        'Default': datasourceDefObj.parameters['Database']
                    },
                    portParam: {
                        'Description' : 'Database port for datasource {datasource_name} (datasource:{datasource_id}, type {type}) to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'Number',
                        'Default': datasourceDefObj.parameters['Port']
                    },
                    hostParam: {
                        'Description' : 'Database host for datasource {datasource_name} (datasource:{datasource_id}, type {type}) to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String',
                        'Default': datasourceDefObj.parameters['Host']                        
                    }
                })
                
                templateDSParameters[datasourceParametersKey]['Database'] = {
                    'Ref': databaseParam
                }    
                templateDSParameters[datasourceParametersKey]['Port'] = {
                    'Ref': portParam
                }
                templateDSParameters[datasourceParametersKey]['Host'] = {
                    'Ref': hostParam
                }        
            else:
                templateDSParameters[datasourceParametersKey]['Host'] = datasourceDefObj.host
                templateDSParameters[datasourceParametersKey]['Port'] = datasourceDefObj.port
                templateDSParameters[datasourceParametersKey]['Database'] = datasourceDefObj.database
             
        
        if dsType == SourceType.REDSHIFT:
            if remap:
                RSclusterIdParam = '{cfnid}{type}ClusterId'.format(cfnid=datasourceIdKey,type=dsType.name)
                appendContent['Parameters'].update({
                    RSclusterIdParam: {
                        'Description' : 'ClusterId for datasource {datasource_name} (datasource:{datasource_id}, type {type}) to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline'
                        .format(datasource_name=datasourceName, datasource_id=datasourceDefObj.id, type=dsType.name),
                        'Type': 'String',
                        'Default': datasourceDefObj.parameters['ClusterId']
                    }
                })
                templateDSParameters[datasourceParametersKey]['ClusterId'] = {
                    'Ref': RSclusterIdParam
                }
            else:
                templateDSParameters[datasourceParametersKey]['ClusterId'] = datasourceDefObj.clusterId
            
        properties['Type'] = dsType.name
        properties['DataSourceParameters'] = templateDSParameters

    
    return appendContent
    


def generateDataSetCFN(datasetObj: QSDataSetDef, datasourceObjs: QSDataSourceDef, tableMap: object, appendContent: dict):
    """
    Function that generates a Cloudformation AWS::QuickSight::DataSet resource https://a.co/5EVM6yD
    synthesized from the source environment account

    Parameters:

    datasetObj(object): Dataset object from the source environment account
    datasourceObjs(list): List of QSDataSourceDef objects
    tableMap (dict): Dictionary of table names and corresponding physical table names
    appendContent(dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements
    
    Returns:

    appendContent(dict): Dictionary containing the definition of Cloudformation template elements    

    Examples:

    >>> generateDataSetCFN(datasetObj=datasetObj, datasourceObjs=datasourceObjs, tableMap=tableMap, appendContent=appendContent)

    """
    
    OPTIONAL_PROPS = ['ColumnGroups', 'FieldFolders', 'RowLevelPermissionTagConfiguration', 'ColumnLevelPermissionRules', 'DataSetUsageConfiguration', 'DatasetParameters']
    
    dependingResources = []
    datasetId = datasetObj.id
    ret = qs.describe_data_set(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=datasetId)

    with open('resources/dataset_resource_CFN_skel.yaml', 'r') as file:
        yaml_dataset = yaml.safe_load(file) 

    dataSetName = ret['DataSet']['Name']

  
    if appendContent is None:
        raise ValueError("Error in createTemplateFromAnalysis:generateDataSetCFN, Append  content is None")
    
    id_sanitized = datasetId.replace('-', '')
    dataSetIdKey = 'DSet{id}'.format(id=id_sanitized)    
    
    
    
    properties = yaml_dataset['Properties']
    
    properties['DataSetId'] = datasetId    
    properties['Name'] = dataSetName

    
    properties['PhysicalTableMap'] = ret['DataSet']['PhysicalTableMap']
    properties['LogicalTableMap'] = ret['DataSet']['LogicalTableMap']
    properties['ImportMode'] = datasetObj.importMode.name  
    
    for datasourceObj in datasourceObjs:
        dependingResources.append(datasourceObj.CFNId)

    for table in tableMap:
        # Dynamically get the child key of PhysicalTableMap's table that could be either RelationalTable, CustomSql, S3Source as per https://shorturl.at/uP124
        physicalTableKey = list(properties['PhysicalTableMap'][table].keys()).pop()
        datasourceArn = properties['PhysicalTableMap'][table][physicalTableKey]['DataSourceArn']
        datasourceId = datasourceArn.split('/')[-1]
        datasourceArnSubStr = {
            'Fn::Sub': 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:datasource/${datasource}'.replace('${datasource}', datasourceId)
        }
        properties['PhysicalTableMap'][table][physicalTableKey]['DataSourceArn'] = datasourceArnSubStr

    appendContent['Resources'][dataSetIdKey] = yaml_dataset
    

    appendContent = generateRefreshSchedulesCFN(datasetObj=datasetObj, appendContent=appendContent)

    for property in OPTIONAL_PROPS:
        if property in ret['DataSet'] and bool(ret['DataSet'][property]):
            properties[property] = ret['DataSet'][property]        

    if datasetObj.rlsDSetDef is not None:
        #This dataset contains a RLS dataset, so we need to update properties and dependencies in template accordingly
        rlsDSetId = datasetObj.rlsDSetDef['Arn'].split('dataset/')[-1]
        rslDsetCFNId = 'DSet{id}'.format(id=rlsDSetId.replace('-', ''))
        dependingResources.append('DSet{id}'.format(id=rlsDSetId.replace('-', '')))        
        datasetObj.rlsDSetDef['Arn'] = {
            'Fn::GetAtt': [
                rslDsetCFNId,
                'Arn'
            ]
        }
        appendContent['Resources'][dataSetIdKey]['Properties']['RowLevelPermissionDataSet'] = datasetObj.rlsDSetDef

    appendContent['Resources'][dataSetIdKey]['DependsOn'] = dependingResources

    return appendContent

def generateRowLevelPermissionDataSetCFN( appendContent:dict, targetDatasetIdKey:str, rlsDatasetDef:dict, datasourceOrd:int, lambdaEvent: object):
    """ Helper function that generates the dataset and datasource used to implement the RLS of a source dataset

    Args:        
        appendContent (dict): Dictionary containing the definition of Cloudformation template elements
        targetDatasetIdKey (str): Dataset CFNId this RLS applies to
        rlsDatasetDef (dict): Object defining the RLS dataset to be applied to the target dataset
        datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)
        lambdaEvent (dict): Lambda event that contains optional parameters that alter the CFN resource that will be created, for example the REMAP_DS that, if provided \
                        will generate a parametrized CFN template to replace datasource parameters


    Returns:
        appendContent (dict): Dictionary containing the definition of Cloudformation template elements including the ones processed by this function
        datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)
    """
    ret_refresh_schedules  = []
    rlsDatasetId = rlsDatasetDef['Arn'].split('dataset/')[-1]

    retRLSDSet = qs.describe_data_set(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=rlsDatasetId)
    
    if retRLSDSet['DataSet']['ImportMode'] == ImportMode.SPICE.name:
        importMode = ImportMode.SPICE
        ret_refresh_schedules = qs.list_refresh_schedules(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=rlsDatasetId)
    else:
        importMode = ImportMode.DIRECT_QUERY

    tableKey = list(retRLSDSet['DataSet']['PhysicalTableMap'].keys()).pop()
    # Dynamically get the child key of PhysicalTableMap's table that could be either RelationalTable, CustomSql, S3Source as per https://shorturl.at/uP124
    tableChildKey = list(retRLSDSet['DataSet']['PhysicalTableMap'][tableKey].keys()).pop()
    rlsDatasourceArn = retRLSDSet['DataSet']['PhysicalTableMap'][tableKey][tableChildKey]['DataSourceArn'] 
    rlsDatasourceId = rlsDatasourceArn.split('/')[-1]    
    appendContent, RLSdataSourceDefObj = generateDataSourceCFN(datasourceId=rlsDatasourceId, appendContent=appendContent, index=datasourceOrd, lambdaEvent=lambdaEvent)
    physicalTableKeys= get_physical_table_map_object(retRLSDSet['DataSet']['PhysicalTableMap'])
    RLSdatasetObj = QSDataSetDef(id=rlsDatasetId, name=retRLSDSet['DataSet']['Name'], importMode=importMode,physicalTableMap=physicalTableKeys, placeholdername=retRLSDSet['DataSet']['Name'], refreshSchedules=ret_refresh_schedules)
    RLSdatasetObj.dependingDSources = [RLSdataSourceDefObj]
    appendContent, datasourceOrd = generateDataSetCFN(datasetObj=RLSdatasetObj, datasourceObjs=RLSdatasetObj.dependingDSources, tableMap=RLSdatasetObj.physicalTableMap, appendContent=appendContent, datasourceOrd=datasourceOrd, lambdaEvent=lambdaEvent)    
    appendContent['Resources'][targetDatasetIdKey]['Properties']['RowLevelPermissionDataSet'] = {
        "Arn" : {
            'Fn::Sub': 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:dataset/${datasetId}'.replace('${datasetId}', rlsDatasetId)
        },
        "FormatVersion" : rlsDatasetDef['FormatVersion'],
        "Namespace" : rlsDatasetDef['Namespace'],
        "PermissionPolicy" : rlsDatasetDef['PermissionPolicy'],
        "Status" : rlsDatasetDef['Status']
    }
    
    appendContent['Resources'][targetDatasetIdKey]['DependsOn'].append(RLSdatasetObj.CFNId)

    return appendContent, datasourceOrd

def generateAnalysisFromTemplateCFN(analysisObj: QSAnalysisDef, templateId:str, appendContent: dict):

    """
    Function that generates a Cloudformation AWS::QuickSight::Analysis resource https://a.co/1V5noMj
    synthesized from the source environment account

    Parameters:

    analysisObj(object): Analysis helper object containing all the properties of the analysis we want to build using CFN
    templateId(str): Template Id of the template that will be used as the source for the analysis
    appendContent(dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements

    Returns:

    appendContent(dict): Dictionary containing the definition of Cloudformation template elements

    Examples:

    >>> generateAnalysisFromTemplateCFN(analysisObj=analysisObj, templateId=templateId, appendContent=appendContent)

    """

    analysis_tag = 'UPDATED_{suffix}'.format(suffix=utc_now.strftime('%d-%m-%y-%H-%M-%S'))

    with open('resources/analysis_resource_CFN_skel.yaml', 'r') as file:
        yaml_analysis = yaml.safe_load(file)  

    properties = yaml_analysis['Properties']
    properties['AnalysisId'] = analysisObj.id
    properties['Name'] = analysisObj.name    
    properties['Tags'] = [
        {
            'Key': 'Pipeline',
            'Value': analysisObj.PipelineName
        },
        {
            'Key': 'Updated',
            'Value': analysis_tag
        }
    ]

    sourceTemplateArnJoinObj = {
            'Fn::Sub': 'arn:aws:quicksight:${SrcQSRegion}:${SourceAccountID}:template/{template_id}'.replace('{template_id}', templateId)
    }

    properties['SourceEntity']['SourceTemplate']['Arn'] = sourceTemplateArnJoinObj
    datasets = analysisObj.datasets
    datasetReferencesObjList = []
    for datasetObj in datasets:
        if datasetObj.isRLS:
            # Is the RLS dataset, doesn't need to be included in the analysis definition, continuing
            continue
        datasetReferencesObj = {}        
        datasetArnJoinObj = {
            'Fn::Sub': 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:dataset/{dataset_id}'.replace('{dataset_id}', datasetObj.id)
        }
        datasetReferencesObj['DataSetArn'] = datasetArnJoinObj
        datasetReferencesObj['DataSetPlaceholder'] = datasetObj.placeholdername
        datasetReferencesObjList.append(datasetReferencesObj)
        
    properties['SourceEntity']['SourceTemplate']['DataSetReferences'] = datasetReferencesObjList
    yaml_analysis['DependsOn'] = analysisObj.getDependingDatasets()
    
    appendContent['Resources'][analysisObj.CFNId] = yaml_analysis    

    return appendContent

def generateRefreshSchedulesCFN(datasetObj: QSDataSetDef, appendContent: dict):

    """
    Function that generates a Cloudformation AWS::QuickSight::RefreshSchedule resource https://a.co/74TVBln
    synthesized from the source environment account

    Parameters:

    datasetObj(object): Dataset helper object containing all the properties of the dataset we want to build using CFN
    appendContent(dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements

    Returns:

    appendContent(dict): Dictionary containing the definition of Cloudformation template elements

    Examples:

    >>> generateRefreshSchedulesCFN(datasetObj=datasetObj, appendContent=appendContent)

    """

    if (datasetObj.importMode == ImportMode.SPICE):

        DSETIdSanitized = datasetObj.id.replace('-', '')

        ret = qs.list_refresh_schedules(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=datasetObj.id)

        for schedule in ret['RefreshSchedules']:
            refresh_schedule_id = schedule['ScheduleId']
            retSchedule = qs.describe_refresh_schedule(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=datasetObj.id, ScheduleId=refresh_schedule_id)
            with open('resources/dataset_refresh_schedule_CFN_skel.yaml', 'r') as file:
                yaml_schedule = yaml.safe_load(file)  
            
            yaml_schedule['Properties']['DataSetId'] = datasetObj.id
            yaml_schedule['Properties']['Schedule'] = retSchedule['RefreshSchedule']
            # There is an inconsistency on the describe refresh API and Timezone is not Capitalized so we need this workaround to fix it.
            yaml_schedule['Properties']['Schedule']['ScheduleFrequency']['TimeZone'] = yaml_schedule['Properties']['Schedule']['ScheduleFrequency'].pop('Timezone')
            scheduleFrequency = retSchedule['RefreshSchedule']['ScheduleFrequency']['Interval']
            if scheduleFrequency == 'MONTHLY':
                futurestartAfterTimeTz = utc_now + relativedelta(months=+1)
            elif scheduleFrequency == 'WEEKLY':
                futurestartAfterTimeTz = utc_now + relativedelta(weeks=+1)
            else:
                futurestartAfterTimeTz = utc_now + relativedelta(days=+7)
            # Remove timezone info as it is included separately in the object
            yaml_schedule['Properties']['Schedule']['StartAfterDateTime'] = futurestartAfterTimeTz.strftime('%Y-%m-%dT%H:%M:%SZ')
            scheduleCFNId = 'RSchedule{id}'.format(id=refresh_schedule_id.replace('-', ''))
            appendContent['Resources'][scheduleCFNId] = yaml_schedule            
            appendContent['Resources'][scheduleCFNId]['DependsOn'] = 'DSet{id}'.format(id=DSETIdSanitized) 
    
    
    return appendContent


def zipAndUploadToS3(bucket: str, files: list, zip_name: str, bucket_owner:str, prefix=None, object_name=None, region='us-east-1', credentials=None):

    """
    Helper function that zips a file and uploads a file to S3 in a particular bucket with a particular key (including a prefix)

    Parameters:

    bucket(str): S3 bucket name
    files(list): Filename to be zipped and uploaded
    zip_name(str): Name of the zip file to be used when zipping the content
    bucket_owner(str): Expected AWS account owning the bucket
    prefix(str): Prefix to be used in the S3 object name
    object_name(str): S3 object name
    region(str): AWS region where the bucket is located
    credentials(dict): AWS credentials to be used in the upload operation

    Returns:

    True if the file was uploaded successfully, False otherwise

    Examples:

    >>> zipAndUploadToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=filename, zip_name=zip_name, prefix=prefix, object_name=object_name, region=region, credentials=credentials)

    """

    with ZipFile(zip_name, 'w') as assetszip:
        for file in files:
            print('Adding file {file} to zip {zip_name}'.format(file=file, zip_name=zip_name))
            assetszip.write(file, arcname=os.path.basename(file))
    
    return uploadFileToS3(bucket=bucket, filename=zip_name, prefix=prefix, object_name=object_name, bucket_owner=bucket_owner, region=region, credentials=credentials)

def uploadFileToS3(bucket: str, filename: str, region: str, bucket_owner:str, prefix=None, object_name=None, credentials=None):

    """
    Helper function that uploads a file to S3 in a particular bucket with a particular key (including a prefix)

    Parameters:

    bucket(str): S3 bucket name
    filename(str): Filename to be uploaded
    prefix(str): Prefix to be used in the S3 object name
    object_name(str): S3 object name
    region(str): AWS region where the bucket is located
    bucket_owner(str): Expected AWS account owning the bucket
    credentials(dict): AWS credentials to be used in the upload operation

    Returns:

    True if the file was uploaded successfully, False otherwise

    Examples:

    >>> uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=filename, prefix=prefix, object_name=object_name, region=region, credentials=credentials)

    """

    if credentials is None:
            s3 = boto3.client('s3', region_name=region)     
    else:
        s3 = boto3.client('s3', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])

    try:
        s3.get_bucket_location(Bucket=bucket, ExpectedBucketOwner=bucket_owner)
    except ClientError as error:
        print('The provided bucket {bucket} doesn\'t belong to the expected account {account_id}'.format(bucket=bucket, account_id=bucket_owner))
        return False

    # If S3 object_name was not specified, use zip_name
    if object_name is None:
        object_name = os.path.basename(filename)
    
    if prefix is not None:
        if prefix[-1] != '/':
            object_name = '{prefix}/{object}'.format(prefix=prefix, object=object_name)
        else:
            object_name = '{prefix}{object}'.format(prefix=prefix, object=object_name)
    
    # Upload the file
    
    try:
        response = s3.upload_file(filename, bucket, object_name)
        print('File {file} uploaded successfully to {bucket} at prefix {prefix}'.format(file=object_name, bucket=DEPLOYMENT_S3_BUCKET, prefix=prefix))
    except ClientError as e:
        logging.error(e)
        print('There was an error uploading file {file} to {bucket} at prefix {prefix}'.format(file=object_name, bucket=DEPLOYMENT_S3_BUCKET, prefix=prefix))
        return False
    return True

#helper function to generate a presigned url in S3 from a given s3 url
def generatePresignedUrl(bucket: str, key:str, region: str, credentials=None):

    """
    Helper function that generates a presigne url in S3 from a given s3 url

    Parameters:

    s3_url(str): S3 url
    region(str): AWS region where the bucket is located
    credentials(dict): AWS credentials to be used in the upload operation

    Returns:

    str: Presigned url

    Examples:

    >>> generatePresignedUrl(s3_url=s3_url, region=region, credentials=credentials)

    """
    if credentials is None:
            s3 = boto3.client('s3', region_name=region)
    else:
        s3 = boto3.client('s3', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    
    # Generate a presigned URL for an S3 object
    expires_in_seconds = 3600

    presigned_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket, 
            'Key': key
        },
        ExpiresIn=expires_in_seconds
    )

    return presigned_url



def get_s3_objects(bucket: str, prefix: str, region: str, credentials=None):

    """
    Helper function that gets all objects in a particular S3 bucket with a particular prefix

    Parameters:

    bucket(str): S3 bucket name
    prefix(str): Prefix to be used in the S3 object name
    region(str): AWS region where the bucket is located
    credentials(dict): AWS credentials to be used in the upload operation

    Returns:

    list: List of files downloaded from S3

    Examples:

    >>> get_s3_objects(bucket=DEPLOYMENT_S3_BUCKET, prefix=prefix, region=region, credentials=credentials)

    """

    downloaded_files = []
    if credentials is None:
            s3 = boto3.client('s3', region_name=region)     
    else:
        s3 = boto3.client('s3', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])

    ret = s3.list_objects(Bucket=bucket, Prefix=prefix)

    for object in ret.get('Contents', []):
        key = object['Key']
        filename = os.path.basename(key)
        local_directory = OUTPUT_DIR
        local_path = os.path.join(local_directory, filename)
        s3.download_file(bucket, key, local_path)
        print('Downloaded {filename} to {local_path}'.format(filename=filename, local_path=local_path))
        downloaded_files.append(local_path)

    return downloaded_files

## Helper function that stores dashboard parameter definition in JSON into a given dynamo db table
def store_dashboard_parameter_definition_in_dynamo(parameter_definition: dict, table_name: str, assetType:str, stage:str, region:str, parameter_help:dict, credentials=None):
    """
    Helper function that stores dashboard parameter definition in JSON into a given dynamo db table

    Parameters:

    parameter_definition(dict): Dashboard parameter definition in JSON
    parameter_help(dict): Parameter dictionary that contains a list of default values, description and type information for each parameter
    table_name(str): Name of the dynamo db table where the parameter definition will be stored
    credentials(dict): AWS credentials to be used in the upload operation
    assetType(str): Type of the asset (valid values are dest or source)
    stage(str): Stage of the dashboard (e.g. dev, prod)    

    Returns:

    True if the parameter definition was stored successfully, False otherwise

    Examples:

    >>> store_dashboard_parameter_definition_in_dynamo(parameter_definition=parameter_definition, table_name=table_name, assetId=assetId, stage=stage, assetType=assetType)

    """

    if assetType not in ['dest', 'source']:
       raise ValueError('Invalid asset type {assetType}, should be either dest or source'.format(assetType=assetType))

    if credentials is None:
        dynamodb = boto3.resource('dynamodb', region_name=region)
    else:
        dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    
    table = dynamodb.Table(table_name)

    try:
        response = table.put_item(
            Item= {
                'AssetType': assetType,
                'StageName': stage,
                'ParameterDefinition': parameter_definition,
                'ParameterDefinitionHelp': parameter_help
            }
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def read_dashboard_parameter_definition_from_dynamo(table_name: str, assetType:str, stage:str, region:str, credentials=None):
    """
    Helper function that reads the QuickSight asset parameter definition from a given DynamoDB table using sts provided credentials

    Parameters:

    table_name(str): Name of the dynamo db table where the parameter definition is stored
    credentials(dict): AWS credentials to be used in the upload operation
    assetType(str): Type of the asset (valid values are dest or source)
    stage(str): Stage of the dashboard (e.g. dev, prod)
    


    Returns:

    parameter_definition(dict): Dashboard parameter definition in JSON

    Examples:

    >>> read_dashboard_parameter_definition_from_dynamo(table_name=table_name, assetId=assetId, stage=stage, assetType=assetType)

    """

    if credentials is None:
        dynamodb = boto3.resource('dynamodb', region_name=region)
    else:
        dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])

    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={
                'AssetType': assetType,
                'StageName': stage
            }
        )
        # filter out items based on assetType
        if 'Item' not in response:
            raise ValueError('No configuration item found in the response when querying {table} parameter table for stage {stage} and assetId {assetId}'.format(table=table_name, stage=stage, assetId=assetType))
        parameter_definition = json.loads(response['Item']['ParameterDefinition'])
    except ClientError as e:
        logging.error(e)
        return {}

    return parameter_definition

def read_all_assetIds_from_dynamo(region:str, credentials=None, table_name=TRACKED_ASSETS_TABLE_NAME):
    """
    Helper function that reads all the assetIds defined in a given DynamoDB table and returns the set of items

    Parameters:

    region(str): The AWS region where the table is located
    credentials(dict): AWS credentials to be used in the upload operation
    table_name(str): Name of the dynamo db table where the parameter definition is stored, defaults to the TRACKED_ASSETS_TABLE_NAME

    Returns:

    assetIds(set): List of assetIds

    Examples:

    >>> read_all_assetIds_from_dynamo(table_name=table_name, credentials=credentials)

    """

    if credentials is None:
        dynamodb = boto3.resource('dynamodb', region_name=region)
    else:
        dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])

    table = dynamodb.Table(table_name)

    try:
        response = table.scan()
        assetIds = [item['AssetId'] for item in response['Items']]
    except ClientError as e:
        logging.error(e)

    return set(assetIds)

# helper function to validate if a given asset Id is a QuickSight dashboard
def validate_asset_id(assetId:str, region:str, credentials=None):
    """
    Helper function that validates if a given asset Id is a QuickSight dashboard

    Parameters:

    assetId(str): Asset ID of the dashboard
    region(str): The AWS region where the table is located
    credentials(dict): AWS credentials to be used in the upload operation

    Returns:

    True if the assetId is a QuickSight dashboard, False otherwise

    Examples:

    >>> validate_asset_id(assetId=assetId, region=region, credentials=credentials)

    """

    quicksight = boto3.client('quicksight', region_name=region)
    
    try:
        response = quicksight.describe_dashboard(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DashboardId=assetId)
    except quicksight.exceptions.ResourceNotFoundException as e:
        print('The assetId {assetId} configured in the source DDB parameter table is not a QuickSight dashboard or the IAM role used by the function doesn''t have access to it, please fix and retry.'.format(assetId=assetId))
        print('At the moment only dashboard objects are supported in the code for this Guidance')
        return False
    
    return True

def writeToFile(filename: str, content: object, format="yaml"):
    """
    Helper function that writes the contents of the object to a file

    Parameters:

    filename(str): Filename to be written
    content(dict): Dictionary with the content to be written to the file

    Returns:

    filename(str): The path of the file written

    Examples:

    >>> writeToFile(filename=filename, content=content)

    """
    
    with open(filename, 'w+') as file:
        if format == 'yaml':
            yaml.dump(content, file)
        elif format == 'json':
            json.dump(content, file, indent=2)
            
    
    return filename

def assumeRoleInDeplAccount(role_arn):
    """
    Helper function that assumes a role in the deployment account

    Parameters:

    role_arn(str): Role ARN to be assumed

    Returns:

    credentials(dict): AWS credentials to be used in the upload operation

    Examples:

    >>> assumeRoleInDeplAccount(role_arn=role_arn)

    """    

    sts_client = boto3.client('sts')

    # The session name to identify the temporary session
    session_name = 'QSAutomationSession'

    # Assume the role
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        ExternalId=ASSUME_ROLE_EXT_ID
    )

    return response['Credentials']

def summarize_template(template_content: dict, templateName: str, s3Credentials: dict, conf_files_prefix: str):
    """
    Helper function that summarizes the template content showing the parameters that need to be filled and provided

    Parameters:

    template_content(dict): Template content in YAML
    templateName(str): Name of the template
    s3Credentials(dict): Credentials to be used to upload files to S3 in deployment account
    conf_files_prefix(str): Prefix in S3 bucket where config files are stored

    Returns:

    parameters_info(dict): Dictionary with the parameters and their description

    Examples:

    >>> summarize_template(template_content=template_content)

    """    
    DIVIDER_SECTION = "----------------------------------------------------------\n"
    if 'Parameters' not in template_content:
        print(DIVIDER_SECTION)
        print("Template {template_name} doesn't contain any parameters, nothing to do in this file".format(template_name=templateName))
        print(DIVIDER_SECTION)
        return {}
    
    parameters_info = template_content['Parameters']

    paramFilename = '{output_dir}/{template_name}_README.json'.format(output_dir=OUTPUT_DIR, template_name=templateName)
    
    writeToFile(content=parameters_info, filename=paramFilename, format="json")
    uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=paramFilename, region=AWS_REGION, object_name='{template_name}_README.json'.format(template_name=templateName), 
                   prefix=conf_files_prefix, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=s3Credentials)

    print(DIVIDER_SECTION)
    print("Template {template_name} contains parameters that need to be set in CodePipeline's CloudFormation artifact. These can be configured in the DynamoDB table in your DEPLOYMENT" \
           "account ({deployment_account_id}) {ddb_param_table} each development stage two records in this table with a different AssetType attribute value (source and dest)."\
           "You will need to open this table in the DDB console and edit the records for each of the environments ({environments}) filling the ParameterDefinition attribute as"\
           "needed and then execute this function with \"MODE\" : \"DEPLOY\" key present in the lambda event. Refer to https://a.co/0DrKhVm for more information on how to use this file"
          .format(template_name=templateName, ddb_param_table=PARAMETER_DEFINITION_TABLE_NAME, deployment_account_id=DEPLOYMENT_ACCOUNT_ID, environments=parameters_info.keys()))
    print("You can access {ddb_param_table} table directly on the console using this link in your DEPLOYMENT_ACCOUNT ({deployment_account_id}): "\
           "https://{region}.console.aws.amazon.com/dynamodbv2/home?region={region}#item-explorer?fromTables=true&maximize=true&table={ddb_param_table}"
          .format(region=AWS_REGION, ddb_param_table=PARAMETER_DEFINITION_TABLE_NAME, deployment_account_id=DEPLOYMENT_ACCOUNT_ID))
    print("Find below a list of the parameters needed for each stack (source and dest) this information is also available under the ParameterDefinitionHelp attribute for each DynamoDB record in the {ddb_param_table} table"
          .format(ddb_param_table=PARAMETER_DEFINITION_TABLE_NAME))
    print("")
    for parameter in parameters_info.keys():
        print("{parameter}: {description}".format(parameter=parameter, description=parameters_info[parameter]['Description']))

    param_formatted = [ 'ParameterKey={parameter},ParameterValue='.format(parameter=parameter)  for parameter in parameters_info.keys() ]
    print("")
    print("Remember that you can still define parameter override values in CodePipeline deploy actions following this format, however for scalability configuration files are recommended to be used instead:")
    print('\n'.join(param_formatted))
    print(DIVIDER_SECTION)

    return parameters_info

def generate_cloudformation_template_parameters(template_content: dict):
    """
    Helper function that generates the CloudFormation template parameters object to be used to generate the file and upload it to S3 so it
    can be filled by the devops team

    Parameters:

    template_content(dict): Template content in YAML

    Returns:

    parameter_list(list): List with parameter objects to be filled up by the user

    Examples:

    >>> generate_cloudformation_template_parameters(template_content=template_content)

    """
    parameter_list = []
    parameter_obj = {}

    if 'Parameters' not in template_content:
        print('No parameters found in template skipping')
        return parameter_list

    parameters = template_content['Parameters']

    for parameter in parameters.keys():
        parameter_obj = {}
        if parameters[parameter]['Type'] == 'Number':
            parameter_obj['ParameterKey'] = parameter
            parameter_obj['ParameterValue'] = "1234"            
        else:
            parameter_obj['ParameterKey'] = parameter
            parameter_obj['ParameterValue'] = "<fill_me>"
        parameter_list.append(parameter_obj)

    return parameter_list

def check_parameters_cloudformation(template_param_list, region, credentials, assetType):
    """
    Helper function that checks if the parameters defined in the template are the same as the ones defined in the DynamoDB parameter definition table

    Parameters:

    template_param_list(list): List with parameter objects to be filled up by the user
    region(str): The AWS region where the table is located
    credentials(dict): AWS credentials to be used in the upload operation
    assetType(str): Type of asset, either 'dest' or 'source'    

    Returns:

    None

    Examples:

    >>> check_parameters_cloudformation(template_param_list=template_param_list, region=region, credentials=credentials, assetType=assetType, assetId=assetId)

    """
    
    if assetType not in ['dest', 'source']:
        raise ValueError('Invalid asset type {assetType}, should be either dest or source'.format(assetType=assetType))
    
    
    deployment_stages = STAGES_NAMES.split(",")[1:]
    for stage in deployment_stages:
        stage = stage.strip()
        print('Checking {asset_type} parameters for stage {stage}'.format(stage=stage, asset_type=assetType))
        key = '{prefix}/{asset_type}_cfn_template_parameters_{stage}.txt'.format(prefix=CONFIGURATION_FILES_PREFIX, asset_type=assetType, stage=stage.strip())
        file_param_obj = read_dashboard_parameter_definition_from_dynamo(table_name=PARAMETER_DEFINITION_TABLE_NAME, assetType=assetType, stage=stage, region=region, credentials=credentials)
        
        file_param_object_keys = []
        template_param_object_keys = []

        for parameter in file_param_obj:
            file_param_object_keys.append(parameter['ParameterKey'])
        
        for parameter in template_param_list:
            template_param_object_keys.append(parameter['ParameterKey'])
        
        if set(file_param_object_keys) != set(template_param_object_keys):
            if set(file_param_object_keys) > set(template_param_object_keys):
                parameters_in_error = set(file_param_object_keys) - set(template_param_object_keys)
                error = 'Not all the parameters configured in your DynamoDB parameter definition table {table} for stage {stage} and assetType {asset_type} are needed in CFN... \
                        Extra parameters in DynamoDB table are {parameters_in_error}. Please, correct file and try again, consider running MODE: ''INITIALIZE'' to fix this.'\
                        .format(table=PARAMETER_DEFINITION_TABLE_NAME, stage=stage, asset_type=assetType, parameters_in_error=parameters_in_error)
            else:
                parameters_in_error = set(template_param_object_keys) - set(file_param_object_keys)
                error = 'Not all the needed CFN parameters were found in your DynamoDB parameter definition table {table} for stage {stage} and assetType {asset_type}...\
                      Missing parameters in DynamoDB table are {parameters_in_error}. Please, add them and try again'.format(table=PARAMETER_DEFINITION_TABLE_NAME, stage=stage, asset_type=assetType,
                                                                                                                  parameters_in_error=parameters_in_error)
            print(error)
            raise ValueError(error)
        else:
            print('All the needed CFN parameters were found in the DynamoDB parameter definition table {table} for stage {stage} and assetType {asset_type}'
                  .format(table=PARAMETER_DEFINITION_TABLE_NAME, stage=stage, asset_type=assetType))
            
            param_file_path = writeToFile('{output_dir}/{asset_type}_cfn_template_parameters_{stage}.txt'.format(output_dir=OUTPUT_DIR, asset_type=assetType, stage=stage.strip()), content=file_param_obj, format='json')            
            uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=param_file_path, region=region, object_name=os.path.basename(key), prefix=CONFIGURATION_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=credentials)
    

    return

def get_physical_table_map_object(physical_table_map:dict):
    """
    Helper function that returns a list of physical table keys

    Parameters:

    physical_table_map(dict): Physical table map as returned by describe_data_set QS API method

    Returns:

    list: List of physical table keys in the expected format of the rest of the helper functions

    Examples:

    >>> get_physical_table_map_object(physical_table_map=physical_table_map)

    """    
    
    physicalTableKeys = []
    for table in physical_table_map:
        physicalTableKey = table
        physicalTableKeys.append(physicalTableKey)
    
    return physicalTableKeys

def json_to_yaml(json_file, yaml_file):
    """
    Helper function that converts a JSON file to YAML

    Parameters:

    json_file(str): Path to JSON file
    yaml_file(str): Path to YAML file

    Returns:

    None

    Examples:

    >>> json_to_yaml(json_file=json_file, yaml_file=yaml_file)

    """    
    
    # Read the JSON file and convert it to YAML format
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    with open(yaml_file, 'w') as f:
        yaml.dump(data, f)
    
    return yaml_file

def add_permissions_to_AAB_resources(template_content:dict):
    """
    Helper function that adds permissions to the AAB resources

    Parameters:

    template_content(dict): Template content in YAML

    Returns:

    template_content(dict): Template content in YAML with permissions added

    Examples:

    >>> add_permissions_to_AAB_resources(template_content=template_content)

    """    
    # Get the list of AAB resources
    aab_resources = template_content['Resources']

    with open('resources/datasource_resource_CFN_skel.yaml', 'r') as file:
        yaml_datasource = yaml.safe_load(file) 
        datasource_permissions_obj = yaml_datasource['Properties']['Permissions']  

    with open('resources/dataset_resource_CFN_skel.yaml', 'r') as file:
        yaml_dataset = yaml.safe_load(file) 
        dataset_permissions_obj = yaml_dataset['Properties']['Permissions']  
    
    with open('resources/analysis_resource_CFN_skel.yaml', 'r') as file:
        yaml_analysis = yaml.safe_load(file) 
        analysis_permissions_obj = yaml_analysis['Properties']['Permissions']  

    with open('resources/theme_resource_CFN_skel.yaml', 'r') as file:
        yaml_theme = yaml.safe_load(file)
        theme_permissions_obj = yaml_theme['Properties']['Permissions']

    updated = False

    for resourceId in aab_resources.keys():
        resource = aab_resources[resourceId]
        resourceType = resource['Type']
        if resourceType in ['AWS::QuickSight::Analysis', 'AWS::QuickSight::DataSet', 'AWS::QuickSight::DataSource', 'AWS::QuickSight::Theme']:
            updated = True            
        if resource['Type'] == 'AWS::QuickSight::Analysis':
            resource['Properties']['Permissions'] = copy.deepcopy(analysis_permissions_obj)
        elif resource['Type'] == 'AWS::QuickSight::DataSet':
            resource['Properties']['Permissions'] = copy.deepcopy(dataset_permissions_obj)
        elif resource['Type'] == 'AWS::QuickSight::DataSource':
            resource['Properties']['Permissions'] = copy.deepcopy(datasource_permissions_obj)
        elif resource['Type'] == 'AWS::QuickSight::Theme':
            resource['Properties']['Permissions'] = copy.deepcopy(theme_permissions_obj)
    
    if updated:
        template_content['Parameters']['QSUser'] = {
            'Description': 'QS Username to provide access to assets in Account where the assets will be created',
            'Type': 'String'
        }
        template_content['Parameters']['DstQSAdminRegion'] = {
            'Description': 'Admin region for your QS dest account where your users are hosted',
            'Type': 'String'
        }

    return template_content    

def generate_template_outputs(analysis_obj:QSAnalysisDef, source_template_content:dict, dest_template_content:dict):
    """
    Helper function that generates the CloudFormation template outputs section to populate to be used to generate the file and upload it to S3 so it
    can be filled by the devops team

    Parameters:

    analysis_obj(QSAnalysisDef): Analysis object as returned by describe_analysis QS API method
    source_template_content(dict): Source Template content in YAML
    dest_template_content(dict): Dest Template content in YAML

    Returns:

    source_template_content(dict): Source Template content in YAML with outputs appended
    dest_template_content(dict): Dest Template content in YAML with outputs appended

    Examples:

    >>> generate_template_outputs(analysis_obj=analysis_obj, source_template_content=source_template_content,  dest_template_content=dest_template_content)

    """    
    source_template_content['Outputs'] = {
        'TemplateId': {
            'Description': 'Id of the QuickSight Template that models the analysis provided as input to the lambda synthesizer function',
            'Value': analysis_obj.TemplateId}
    }
    dest_template_content['Outputs'] = {
        'AnalysisURL': {
            'Description': 'URL of the QuickSight Analysis modeled by the QuickSight template, will be the same id for all the stages',
            'Value': 'https://{region}.quicksight.aws.amazon.com/sn/analyses/{analysis_id}'.format(region=AWS_REGION, analysis_id=analysis_obj.id)
        }
    }

    return source_template_content, dest_template_content

def generate_cloud_formation_override_list_AAB(analysisObjList:QSAnalysisDef):
    """
    Helper function that generates the CloudFormation template override object to be used in the start_asset_bundle_export_job
    API call

    Parameters:

    analysisObjList [QSAnalysisDef]: List of Analysis object as returned by describe_analysis QS API method

    Returns:

    CloudFormationOverridePropertyConfiguration(object): Object with overrides to use in start_asset_bundle_export_job

    Examples:

    >>> generate_cloud_formation_override_list_AAB(analysisObj=analysisObj)

    """    
    vpc_conn_arns = []
    VPCConnectionOverridePropertiesList = []
    refresh_schedules_arns = []
    RefreshScheduleOverridePropertiesList = []
    datasource_arns = []
    DataSourceOverridePropertiesList = []
    
    for analysisObj in analysisObjList:    
        for dataset in analysisObj.datasets:
            if dataset.refreshSchedules != []:
                for schedule in dataset.refreshSchedules:
                    refresh_schedules_arns.append(schedule['Arn'])
                    RefreshScheduleOverridePropertyObj = {
                        'Arn': schedule['Arn'],
                        'Properties': ['StartAfterDateTime']
                    }
                    RefreshScheduleOverridePropertiesList.append(RefreshScheduleOverridePropertyObj)
                
            for datasource in dataset.dependingDSources:
                if datasource.arn not in datasource_arns:
                    datasource_arns.append(datasource.arn)
                else:
                    print('generate_cloud_formation_override_list_AAB: Skipping datasource {datasource_name} as it has been already processed'.format(datasource_name=datasource.name))
                    continue
                if (isinstance(datasource, QSRDSDatasourceDef) or isinstance(datasource, QSRDBMSDatasourceDef)) and datasource.vpcConnectionArn != '':
                    vpc_conn_arns.append(datasource.vpcConnectionArn)
                properties = []
                # TODO add support for other datasource types
                if isinstance(datasource, QSRDSDatasourceDef):
                    properties = ['SecretArn','Username','Password','InstanceId', 'Database']
                if isinstance(datasource, QSRDBMSDatasourceDef):
                    properties = ['SecretArn','Username','Password','Host', 'Database']
                    if datasource.type == SourceType.REDSHIFT:
                        properties.append('ClusterId')
                if isinstance(datasource, QSServiceDatasourceDef):
                    if datasource.type == SourceType.S3:
                        properties = ['ManifestFileLocation']
                    if datasource.type == SourceType.ATHENA:
                        properties = ['WorkGroup']
                DataSourceOverridePropertyObj = {
                    'Arn': datasource.arn,
                    'Properties': properties
                }
                DataSourceOverridePropertiesList.append(DataSourceOverridePropertyObj)

    #remove any duplicates
    vpc_conn_arns = list(set(vpc_conn_arns))

    for vpc_conn_arn in vpc_conn_arns:
        VPCConnectionOverridePropertyObj = {
                'Arn': vpc_conn_arn,
                'Properties': ['Name','DnsResolvers','RoleArn']
        }
        VPCConnectionOverridePropertiesList.append(VPCConnectionOverridePropertyObj)

    CloudFormationOverridePropertyConfiguration={
        'ResourceIdOverrideConfiguration': {
            'PrefixForAllResources': False
        },
        'VPCConnections': VPCConnectionOverridePropertiesList,
        'RefreshSchedules': RefreshScheduleOverridePropertiesList,
        'DataSources': DataSourceOverridePropertiesList
    }
    
    if len(vpc_conn_arns) == 0:
        del CloudFormationOverridePropertyConfiguration['VPCConnections']
    
    if len(refresh_schedules_arns) == 0:
        del CloudFormationOverridePropertyConfiguration['RefreshSchedules']

    return CloudFormationOverridePropertyConfiguration

def replicate_dashboard_via_template(analysisObjList:list, remap):
    """
    Helper function that replicates a QuickSight dashboard using a template and also create assets for all the depending assets (datasets, datasources and secrets)

    Parameters: 
    
    analysisObjList(List[QSAnalysisDef]): List of Analysis objects 
    remap(Boolean): Whether or not the datasource definitions should be remapped (always True for AAB)    
    Returns:

    source_account_yaml, dest_account_yaml YAML objects representing the generated templates (source and destination)


    Examples:

    >>> replicate_dashboard_template(analysisObj, remap)

    """    
    
    dest_account_yaml = {}
    source_account_yaml = {}

    with open('resources/dest_CFN_skel.yaml', 'r') as file:
        dest_account_yaml = yaml.safe_load(file)
    with open('resources/source_CFN_skel.yaml', 'r') as file:
        source_account_yaml = yaml.safe_load(file) 

    dest_account_yaml['Resources'] = {}
    source_account_yaml['Resources'] = {}
    analysisIndex = 0
    for analysisObj in analysisObjList:        
        print("Item {index}/{total}: Replicating dashboard {dashboard_id} from analysis {analysis_id} ..."
              .format(index=analysisIndex+1, total=len(analysisObjList),dashboard_id=analysisObj.AssociatedDashboardId, analysis_id=analysisObj.id))

        datasets = analysisObj.datasets
        # TODO: Check logic
        for datasetDefObj in datasets:        
            
            for datasourceDefObj in datasetDefObj.dependingDSources:
                try:
                    dest_account_yaml = generateDataSourceCFN(datasourceDefObj=datasourceDefObj, appendContent=dest_account_yaml, remap=remap)
                except ValueError as error:
                    print(error)
                    print('There was an issue creating the following datasource: {datasourceId} cannot proceed further'.format(datasourceId=datasourceDefObj.id))
                    return {
                    'statusCode': 500
                    }            

        source_account_yaml = generateQSTemplateCFN(analysisDefObj=analysisObj, appendContent=source_account_yaml)   
        
        for datasetObj in analysisObj.datasets:
            dest_account_yaml = generateDataSetCFN(datasetObj=datasetObj, datasourceObjs=datasetObj.dependingDSources, tableMap=datasetObj.physicalTableMap, appendContent=dest_account_yaml)

        dest_account_yaml = generateAnalysisFromTemplateCFN(analysisObj=analysisObj, templateId=analysisObj.TemplateId, appendContent=dest_account_yaml)

        source_account_yaml, dest_account_yaml = generate_template_outputs(analysis_obj=analysisObj, source_template_content=source_account_yaml, dest_template_content=dest_account_yaml)
        analysisIndex = analysisIndex + 1

    
    return source_account_yaml, dest_account_yaml

def replicate_dashboard_via_AAB(analysisObjList:list, remap):
    """
    Helper function that replicates a QuickSight dashboard using a assets as bundle and outputs results in CLOUDFORMATION_JSON 

    Parameters: 
    
    analysisObjList(List[QSAnalysisDef]): List of Analysis objects 
    remap(Boolean): Whether or not the datasource definitions and other properties should be remapped (more info here https://a.co/g1Tf0fp)
    
    Returns:

    source_account_yaml, dest_account_yaml YAML objects representing the generated templates (source and destination)

    Examples:

    >>> replicate_dashboard_template(analysisObj, event)

    """   

    MAX_RETRIES = 5
    now = datetime.now()    
    EXPORT_JOB_ID = 'QS_CI_CD_EXPORT_{suffix}'.format(suffix=now.strftime('%d-%m-%y-%H-%M-%S'))
    EXPORT_TERMINAL_STATUSES = ['SUCCESSFUL', 'FAILED']
    initial_wait_time_sec = 5

    resourceArns = [ analysis.arn for analysis in analysisObjList]

    if remap:
        CloudFormationOverridePropertyConfiguration = generate_cloud_formation_override_list_AAB(analysisObjList=analysisObjList)   
        ret = qs.start_asset_bundle_export_job (AwsAccountId=FIRST_STAGE_ACCOUNT_ID, AssetBundleExportJobId=EXPORT_JOB_ID, ResourceArns=resourceArns, IncludeAllDependencies=True, 
                                      ExportFormat='CLOUDFORMATION_JSON', CloudFormationOverridePropertyConfiguration=CloudFormationOverridePropertyConfiguration)
    else:
        ret = qs.start_asset_bundle_export_job (AwsAccountId=FIRST_STAGE_ACCOUNT_ID, AssetBundleExportJobId=EXPORT_JOB_ID, ResourceArns=resourceArns, IncludeAllDependencies=True, 
                                      ExportFormat='CLOUDFORMATION_JSON', ValidationStrategy={'StrictModeForAllResources':False})
    
    # Check progress

    

    while MAX_RETRIES > 0:
        MAX_RETRIES = MAX_RETRIES - 1
        ret = qs.describe_asset_bundle_export_job(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, AssetBundleExportJobId=EXPORT_JOB_ID)
        if ret['JobStatus'] in EXPORT_TERMINAL_STATUSES:
            break
        print('Assets as Bundle export job with id {id} is currently in a non terminal status ({status}) waiting for {seconds} seconds'.format(id=EXPORT_JOB_ID, status=ret['JobStatus'], seconds=initial_wait_time_sec))
        time.sleep(initial_wait_time_sec)
        initial_wait_time_sec = initial_wait_time_sec * 2

    if ret['JobStatus'] == 'FAILED':
        raise ValueError('Export job with ID {id} failed with error {error}, cannot continue'.format(id=EXPORT_JOB_ID, error=ret['Errors']))
    
    downloadURL = ret['DownloadUrl']

    json_filename = '{output_dir}/{export_job_id}_CFN_bundle.json'.format(output_dir=OUTPUT_DIR, export_job_id=EXPORT_JOB_ID)
    yaml_filename = '{output_dir}/{export_job_id}_CFN_bundle.yaml'.format(output_dir=OUTPUT_DIR, export_job_id=EXPORT_JOB_ID)
    
    if downloadURL.lower().startswith('http'):
        ret = urlretrieve(downloadURL, json_filename)
    else:
        raise ValueError('Illegal scheme in downloadURL ({downloadURL}) should be http(s). Aborting ...'.format(downloadURL=downloadURL))

    json_to_yaml(json_file=json_filename, yaml_file=yaml_filename)

    with open('resources/dummy_CFN_skel.yaml', 'r') as file:
        source_account_yaml = yaml.safe_load(file)
    
    with open(yaml_filename, 'r') as file:
        dest_account_yaml = yaml.safe_load(file)
    
    return source_account_yaml, dest_account_yaml

# helper function that takes a cloudformation stack definition and returns a list of objects mapping the CFN resource Id and the QS resource Id
def generate_resource_id_mapping(template_content:dict):
    """
    Helper function that takes a cloudformation stack definition and returns a list of objects mapping the CFN resource Id and the QS resource Id

    Parameters:

    template_content(dict): Cloudformation stack definition in yaml

    Returns:

    resourceIdMapping(List[dict]): List of objects mapping the CFN resource Id and the QS resource Id

    Examples:

    >>> generate_resource_id_mapping(template_content)

    """
    resourceIdMapping = []
    template_resources = template_content['Resources']
    RESOURCES_TO_MAP = ['AWS::QuickSight::DataSource', 'AWS::QuickSight::DataSet', 'AWS::QuickSight::Analysis', 'AWS::QuickSight::VPCConnection', 'AWS::QuickSight::Theme']

    for resource_key in template_resources.keys():
        resource = template_resources[resource_key]
        if resource['Type'] in RESOURCES_TO_MAP:
            resource_type = resource['Type'].split('::')[-1]
            resource_map = {
                'CFNId': resource_key,
                'ResourceId': resource['Properties']['{resource_type}Id'.format(resource_type=resource_type)],
                'ResourceType': resource_type
            }
            resourceIdMapping.append(resource_map)

    return resourceIdMapping

# helper function that gets a CFNId reference and a resource_id_mapping object and returns the mapped resource
def get_mapped_resource(cfnId:str, resource_id_mapping: dict):
    """
    Helper function that gets a CFNId reference and a resource_id_mapping object and returns the mapped resource

    Parameters:

    cfnId(str): CFNId reference
    resourceIdMapping(List[dict]): List of objects mapping the CFN resource Id and the QS resource Id

    Returns:

    mappedResource(dict): Object mapping the CFN resource Id and the QS resource Id

    Examples:

    >>> get_mapped_resource(cfnId, resourceIdMapping)

    """
    mappedResource = [mapping for mapping in resource_id_mapping if mapping['CFNId'] == cfnId].pop()

    return mappedResource

# helper function that takes a cloudformation stack definition and changes all CFN object references with Ids so it can be splitted
def change_stack_references_to_ids(template_content:dict, resource_id_mapping: dict):
    """
    Helper function that takes a cloudformation stack definition and changes all CFN object references with Ids so it can be splitted

    Parameters:

    template_content(dict): Cloudformation stack definition in yaml
    resourceIdMapping(List[dict]): List of objects mapping the CFN resource Id and the QS resource Id
    
    Returns:

    template_content(dict): Cloudformation stack definition in yaml with all references changed to ids

    Examples:

    >>> change_stack_references_to_ids(template_content)

    """
    template_resources = template_content['Resources']
    RESOURCES_TO_CHANGE = ['AWS::QuickSight::DataSource', 'AWS::QuickSight::DataSet', 'AWS::QuickSight::Analysis']
    SUPPORTED_PHYSICAL_TABLE_TYPES = ['CustomSql','RelationalTable', 'S3Source']

    for resource_key in template_resources.keys():
        resource = template_resources[resource_key]
        if resource['Type'] in RESOURCES_TO_CHANGE:
            # Process analysis objects
            if resource['Type'] == 'AWS::QuickSight::Analysis':
                # replace DataSetArn references for each dataset              
                for datasetIdDeclaration in resource['Properties']['Definition']['DataSetIdentifierDeclarations']:
                    expectedType = 'DataSet'
                    referenceId = datasetIdDeclaration['DataSetArn']['Fn::GetAtt'][0]
                    mappedResource = get_mapped_resource(referenceId, resource_id_mapping)
                    # The dataset ids in ASSETS AS BUNDLE CFN output are sanitized by taking the first 20 chars of the resource id,  removing '-' characters and appending a hash of 6 extra charcters
                    if mappedResource['ResourceType'] == expectedType:                        
                        datasetIdDeclaration['DataSetArn'] = {
                            'Fn::Sub' : 'arn:${{AWS::Partition}}:quicksight:${{AWS::Region}}:${{AWS::AccountId}}:dataset/{datasetId}'.format(datasetId=mappedResource['ResourceId'])
                        }
                    else:
                        raise ValueError('Invalid Resource Type in resourceIdMapping object, expected type was {expected_type} but type in mapping for resource with id {resource_id} was {actual_type}'
                                         .format(expected_type= expectedType, resource_id = mappedResource['ResourceId'], actual_type = mappedResource['ResourceType']))
                if 'ThemeArn' in resource['Properties']:
                    # Analysis has a ThemeArn reference that we need to replace
                    expectedType = 'Theme'
                    referenceId = resource['Properties']['ThemeArn']['Fn::GetAtt'][0]
                    mappedResource = get_mapped_resource(referenceId, resource_id_mapping)
                    if mappedResource ['ResourceType'] == expectedType:
                        resource['Properties']['ThemeArn'] = {
                            'Fn::Sub' : 'arn:${{AWS::Partition}}:quicksight:${{AWS::Region}}:${{AWS::AccountId}}:theme/{themeId}'.format(themeId=mappedResource['ResourceId'])
                        }
                    else:
                        raise ValueError('Invalid Resource Type in resourceIdMapping object, expected type was {expected_type} but type in mapping for resource with id {resource_id} was {actual_type}'
                                            .format(expected_type= expectedType, resource_id = mappedResource['ResourceId'], actual_type = mappedResource['ResourceType']))
            
            # Process data sources objects that contain a VPC Connection
            if resource['Type'] == 'AWS::QuickSight::DataSource' and 'VpcConnectionProperties' in resource['Properties']:                
                expectedType = 'VPCConnection'
                vpc_connection_properties = resource['Properties']['VpcConnectionProperties']
                referenceId = vpc_connection_properties['VpcConnectionArn']['Fn::GetAtt'][0]
                mappedResource = get_mapped_resource(referenceId, resource_id_mapping)
                if mappedResource ['ResourceType'] == expectedType:
                    vpc_connection_properties['VpcConnectionArn'] = {
                        'Fn::Sub' : 'arn:${{AWS::Partition}}:quicksight:${{AWS::Region}}:${{AWS::AccountId}}:vpcConnection/{vpc_connection_id}'.format(vpc_connection_id=mappedResource['ResourceId'])
                    }
                else:
                    raise ValueError('Invalid Resource Type in resourceIdMapping object, expected type was {expected_type} but type in mapping for resource with id {resource_id} was {actual_type}'
                                         .format(expected_type= expectedType, resource_id = mappedResource['ResourceId'], actual_type = mappedResource['ResourceType']))
            
            # Process dataset objects
            if resource['Type'] == 'AWS::QuickSight::DataSet':
                datasetId = resource['Properties']['DataSetId']
                
                expectedType = 'DataSource'
                for physicalTableMapObjectKey in resource['Properties']['PhysicalTableMap'].keys():
                    physicalTableTypeKeys =  resource['Properties']['PhysicalTableMap'][physicalTableMapObjectKey].keys()
                    physicalTableType = list(physicalTableTypeKeys)[0]
                    referenceId = resource['Properties']['PhysicalTableMap'][physicalTableMapObjectKey][physicalTableType]['DataSourceArn']['Fn::GetAtt'][0]
                    mappedResource = get_mapped_resource(referenceId, resource_id_mapping)
                    if physicalTableType in SUPPORTED_PHYSICAL_TABLE_TYPES:
                        if mappedResource['ResourceType'] == expectedType:
                            resource['Properties']['PhysicalTableMap'][physicalTableMapObjectKey][physicalTableType]['DataSourceArn'] = {
                                'Fn::Sub' : 'arn:${{AWS::Partition}}:quicksight:${{AWS::Region}}:${{AWS::AccountId}}:datasource/{datasource_id}'.format(datasource_id=mappedResource['ResourceId'])
                            }
                        else:
                            raise ValueError('Invalid Resource Type in resourceIdMapping object, expected type was {expected_type} but type in mapping for resource with id {resource_id} was {actual_type}'
                                         .format(expected_type= expectedType, resource_id = mappedResource['ResourceId'], actual_type = mappedResource['ResourceType']))
                    else:
                        raise ValueError('Unsupported Physical Table Type in CFN template, supported types are {supported_types} but type {actual_type} is used in dataset {dataset_id}'
                                         .format(supported_types=SUPPORTED_PHYSICAL_TABLE_TYPES, actual_type=physicalTableType, dataset_id=datasetId))
            
    return template_content

# helper function that returns the group where a given resource id is located in a grouped_resources_content
def get_resource_group(resource_id:str, grouped_resources_content:dict):
    """
    Helper function that returns the group where a given resource id is located in a grouped_resources_content

    Parameters:

    resource_id(str): Resource id to find in grouped_resources_content
    grouped_resources_content(dict): Dictionary of grouped resources to generate nested stacks

    Returns:

    group(str): Group where resource_id is located in grouped_resources_content

    Examples:

    >>> get_resource_group(resource_id, grouped_resources_content)

    """
    sanitized_id = resource_id.replace('-', '')
    # print('Looking for resource id:'+sanitized_id)
    # print('Grouped resources content is:')
    # print(grouped_resources_content)
    for group in grouped_resources_content.keys():
        for CFNresourceId in grouped_resources_content[group]['Resources'].keys():
            if sanitized_id[:20] in CFNresourceId:
                return group

    return None

# helper function that takes a cloudformation stack definition in yaml and splits its resources into nested stacks
def split_stack_resources_and_parameters_into_groups(template_content:dict):
    """
    Helper function that takes a cloudformation stack definition in yaml and splits its resources into groups based on configuration, also parameters for resources are grouped and returned

    Parameters:

    template_content(dict): Cloudformation stack definition in yaml

    Returns:

    grouped_resources_content(dict): Dictionary of grouped resources to generate nested stacks
    grouped_parameters_content(dict): Dictionary of grouped parameters to generate nested stacks
    
    Examples:

    >>> split_stack_into_nested_stacks(template_content)

    """
    resources_to_split = {
        'datasources' : ['AWS::QuickSight::DataSource'],
        'datasets' : ['AWS::QuickSight::DataSet'],
        'analysis' : ['AWS::QuickSight::Analysis'],
        'vpcConnections' : ['AWS::QuickSight::VPCConnection', 'AWS::QuickSight::Theme']
    }

    colocated_resources = {
        'AWS::QuickSight::RefreshSchedule': 'datasets'
    }

    grouped_resources_content = {}
    grouped_parameters_content = {}
    
    parameters_mapping = {
        'datasources' : ['DstQSAdminRegion', 'QSUser'],
        'datasets' : ['DstQSAdminRegion', 'QSUser'],
        'analysis' : ['DstQSAdminRegion', 'QSUser'],
        'vpcConnections' : ['DstQSAdminRegion', 'QSUser']
    }
    
    if REPLICATION_METHOD == 'TEMPLATE':
        parameters_mapping['analysis'] = parameters_mapping['analysis'] + ['SrcQSRegion','SourceAccountID']

    template_resources = template_content['Resources']
    template_parameters = template_content['Parameters']
    added_parameters = ['DstQSAdminRegion', 'QSUser']
    
    MAX_RESOURCES_PER_GROUP = 10
    for resource_type in resources_to_split.keys():
        group_index = 0
        for resource_key in template_resources.keys():            
            resource = template_resources[resource_key]
            # Remove DependsOn elements as now dependencies are managed between stack sets so they are no longer needed exept if the resource type is a colocated resource ...
            if 'DependsOn' in resource.keys() and resource['Type'] not in colocated_resources:
                del(resource['DependsOn'])
            if resource['Type'] in resources_to_split[resource_type]:
                resource_index = '{resource_type}_{index}'.format(resource_type=resource_type, index=group_index)
                if resource_index in grouped_resources_content.keys() and len(grouped_resources_content[resource_index]['Resources']) >= MAX_RESOURCES_PER_GROUP:
                    group_index = group_index + 1
                    resource_index = '{resource_type}_{index}'.format(resource_type=resource_type, index=group_index)
                
                if resource_index not in grouped_resources_content.keys():
                    grouped_resources_content[resource_index] = {}
                    grouped_resources_content[resource_index]['Resources'] = {}
                    grouped_parameters_content[resource_index] = {}
                    grouped_parameters_content[resource_index]['Parameters'] = {}
                
                grouped_resources_content[resource_index]['Resources'][resource_key] = resource
                # adding grouped parameters                
                for parameter in parameters_mapping[resource_type]:
                    grouped_parameters_content[resource_index]['Parameters'][parameter] = template_parameters[parameter]
                
                if resource_type == 'datasources':
                    # we need to add datasource parameters
                    datasourceId = resource['Properties']['DataSourceId']
                    parameter_list = [{key: template_parameters[key]} for key in list(template_parameters) if 'datasource:{datasource_id}'.format(datasource_id=datasourceId) in template_parameters[key]['Description']]
                    for parameter in parameter_list:
                        param_key = list(parameter.keys())[0]
                        grouped_parameters_content[resource_index]['Parameters'][param_key] = parameter[param_key]
                        added_parameters.append(param_key)
                if resource_type == 'vpcConnections' and resource['Type'] == 'AWS::QuickSight::VPCConnection':
                    # we need to add vpc connection parameters
                    vpcConnectionId = resource['Properties']['VPCConnectionId']
                    parameter_list = [{key: template_parameters[key]} for key in list(template_parameters) if 'vpcConnection:{vpc_connection_id}'.format(vpc_connection_id=vpcConnectionId) in template_parameters[key]['Description']]
                    for parameter in parameter_list:
                        param_key = list(parameter.keys())[0]
                        grouped_parameters_content[resource_index]['Parameters'][param_key] = parameter[param_key]
                        added_parameters.append(param_key)

            elif resource['Type'] in colocated_resources and resource_type == colocated_resources[resource['Type']]:
                # needs to be colocated and is a RefreshSchedule
                if resource['Type'] == 'AWS::QuickSight::RefreshSchedule':
                    # we need to add to the datasets group
                    datasetId = resource['Properties']['DataSetId']
                    refreshScheduleId = resource['Properties']['Schedule']['ScheduleId']
                    target_group = get_resource_group(datasetId, grouped_resources_content)
                    grouped_resources_content[target_group]['Resources'][resource_key] = resource
                    parameter_list = [{key: template_parameters[key]} for key in list(template_parameters) if 'refresh-schedule:{schedule_id}'.format(schedule_id=refreshScheduleId) in template_parameters[key]['Description']]
                    for parameter in parameter_list:
                        param_key = list(parameter.keys())[0]
                        grouped_parameters_content[target_group]['Parameters'][param_key] = parameter[param_key]
                        added_parameters.append(param_key)

    
    return grouped_resources_content, grouped_parameters_content

# helper function that takes a dictionary of grouped resources and creates a cloudformation stack for each of the groups and persist the file in yaml format locally
def generate_nested_stacks_from_grouped_resources(grouped_resources_content:dict, grouped_parameters_content:dict, credentials:object):
    """
    Helper function that takes a dictionary of grouped resources and creates a cloudformation stack for each of the groups and persist the file in yaml format locally

    Parameters:

    grouped_resources_content(dict): Dictionary containing groups of resources    
    grouped_parameters_content(dict): Dictionary containing groups of parameters    
    credentials(object): Credentials object containing the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to use with S3
    
    Returns:

    root_stack_skel(dict): The root template content in pyyaml format

    Examples:

    >>> generate_nested_stacks_from_grouped_resources(grouped_resources_content, template_id, remap)

    """

    dependencies = {
        'analysis': ['datasets'],
        'datasets': ['datasources'],
        'datasources': ['vpcConnections'],
        'vpcConnections': []
    }
    
    parent_stack_skel = {
            'AWSTemplateFormatVersion': '2010-09-09',
            'Description': 'Parent Stack for QuickSight CI/CD Pipeline {pipeline_name}, nested stacks are being generated because GENERATE_NESTED_STACKS was set to True in the synthesizer lambda function'.format(pipeline_name=PIPELINE_NAME),
            'Resources': {},
    }
    
    nested_stack_skel = {
            'AWSTemplateFormatVersion': '2010-09-09',
            'Resources': {},
            'Description': 'Nested Stack for {group_name}'            
    }

    all_parameters = {}
    

    for group in grouped_parameters_content:
        group_parameters = grouped_parameters_content[group]['Parameters']
        for parameter_key in group_parameters.keys():
            all_parameters[parameter_key] = group_parameters[parameter_key]


    
    for group in grouped_resources_content.keys():
        # NESTED STACK TEMPLATES GENERATION
        # process the grouped resources content and create one nested stack for each group
        group_resources = grouped_resources_content[group]['Resources']
        group_parameters = grouped_parameters_content[group]['Parameters']
        nested_stack_skel['Resources'] = group_resources
        nested_stack_skel['Description'] = 'Nested Stack for {group_name}'.format(group_name=group)            
        nested_stack_skel['Parameters'] = group_parameters
        
        # write the nested stack skeleton to a file and upload it to a bucket
        filename = '{group_name}.template'.format(group_name=group)
        nested_stack_filename = '{output_dir}/{filename}'.format(output_dir=OUTPUT_DIR, filename=filename)
        writeToFile(nested_stack_filename, nested_stack_skel, format="yaml")
        uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=nested_stack_filename, region=AWS_REGION, object_name=filename,prefix=ASSETS_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=credentials)

        # PARENT TEMPLATE GENERATION
        # process the grouped resources content and create one nested stack for each group in the parent template
        nested_stack_id = 'nestedStack{group_name}'.format(group_name=group.replace('_', ''))
        depending_groups = [ 'nestedStack{group_name}'.format(group_name=x.replace('_','')) for x in grouped_resources_content if x.split('_')[0] in dependencies[group.split('_')[0]]]
        parameters = {}
        filename = '{group_name}.template'.format(group_name=group)
        key= '{prefix}/{key}'.format(prefix=ASSETS_FILES_PREFIX, key=filename)
        # generate a presigned URL
        presignedUrl = generatePresignedUrl(key=key, bucket=DEPLOYMENT_S3_BUCKET, region=AWS_REGION, credentials=credentials)
        templateUrlSkel = 'http://s3.amazonaws.com/{s3Bucket}/{prefix}/{group_name}.template'
        for parameterKey in grouped_parameters_content[group]['Parameters'].keys():
            parameters[parameterKey] = {
                'Ref' : parameterKey
            }

        parent_stack_skel['Resources'][nested_stack_id] = {
            'Type' : 'AWS::CloudFormation::Stack',
            'Properties' : {
                'TemplateURL' : presignedUrl,
                'Parameters' : parameters
            }
        }

        if len(depending_groups) > 0:
            parent_stack_skel['Resources'][nested_stack_id]['DependsOn'] = depending_groups
        
        parent_stack_skel['Parameters'] = all_parameters
    
    return parent_stack_skel

# Helper function that creates an QSAnalysisDef object from the analysis that originated the dashboard ID passed as argument, this object will be then used to generate a cloudformation template to build such analysis
def getAnalysisAssociatedWithDashboard(dashboardId, ds_index):
    """
    Helper function that creates an QSAnalysisDef object from the analysis that originated the dashboard ID passed as argument, this object will be then used to generate a cloudformation template to build such analysis

    Parameters:

    dashboardId(String): Dashboard ID
    ds_index(Integer): Start index (within the generated CFN template) to use for the first (and subsequent) analysis datasources

    Returns:

    analysisObj(QSAnalysisDef): Object encapsulating all the information and depending assets from the analysis
    ds_index(Integer): Index (within the generated CFN template) of the last datasource in the analysis

    Examples:

    >>> getAnalysisAssociatedWithDashboard(dashboardId)

    """

    ret = qs.describe_dashboard(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DashboardId=dashboardId)
    source_analysis_arn = ret['Dashboard']['Version']['SourceEntityArn']
    analysis_id = source_analysis_arn.split('analysis/')[1]
    ret = qs.describe_analysis(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, AnalysisId=analysis_id)
    dataset_arns = ret['Analysis']['DataSetArns']
    ds_count = ds_index
    datasourceDefObjList = []
    datasetsDefObjList = []
    analysis_name = ret['Analysis']['Name']
    permissions = qs.describe_analysis_permissions(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, AnalysisId=analysis_id)
    owner = permissions['Permissions'].pop()
    username =  owner['Principal'].split('default/')
    qs_admin_region = owner['Principal'].split(':')[3]
    analysis_arn = ret['Analysis']['Arn']
    analysis_region = analysis_arn.split(':')[3]

    rls_dataset_ids = []

    for datasetarn in dataset_arns:    
        ret_refresh_schedules  = []
        physicalTableKeys = []
        dset_datasources = []
        datasourceDefObjList = []
        datasetId = datasetarn.split('dataset/')[-1]
        ret = qs.describe_data_set(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=datasetId )
        physicalTableKeys= get_physical_table_map_object(ret['DataSet']['PhysicalTableMap'])
        
        importMode = None
        if ret['DataSet']['ImportMode'] == ImportMode.SPICE.name:
            importMode = ImportMode.SPICE
            ret_refresh_schedules = qs.list_refresh_schedules(AwsAccountId=FIRST_STAGE_ACCOUNT_ID, DataSetId=datasetId)
        else:
            importMode = ImportMode.DIRECT_QUERY
        datasetObj = QSDataSetDef(name=ret['DataSet']['Name'], id=datasetId, importMode=importMode, placeholdername=ret['DataSet']['Name'], refreshSchedules=ret_refresh_schedules, physicalTableMap=physicalTableKeys)

        #Get depending datasources
        for physicalTableKey in physicalTableKeys:
                tableTypeKey = list(ret['DataSet']['PhysicalTableMap'][physicalTableKey].keys()).pop()
                datasourceArn = ret['DataSet']['PhysicalTableMap'][physicalTableKey][tableTypeKey]['DataSourceArn']
                datasourceId = datasourceArn.split('datasource/')[-1]
                dset_datasources.append(datasourceId)
        
        #Using set to avoid duplicated datasources to be created (one dataset could use the same datasource several times)
        dset_datasources = list(set(dset_datasources))

        for datasourceId in dset_datasources:
            dataSourceDefObj = {}            
            dataSourceDefObj = generateDataSourceObject(datasourceId=datasourceId, datasourceIndex=ds_count)
            datasourceDefObjList.append(dataSourceDefObj)
            ds_count = ds_count + 1
        
        datasetObj.dependingDSources = datasourceDefObjList
        datasetsDefObjList.append(datasetObj)
        if 'RowLevelPermissionDataSet' in ret['DataSet'] and bool(ret['DataSet']['RowLevelPermissionDataSet']):
            # Dataset contains row level permission, we need add to depending resources  
            dataset_arns.append(ret['DataSet']['RowLevelPermissionDataSet']['Arn'])
            datasetObj.rlsDSetDef = ret['DataSet']['RowLevelPermissionDataSet']
            rls_dataset_ids.append(ret['DataSet']['RowLevelPermissionDataSet']['Arn'].split('dataset/')[-1])
        
        
    analysis = QSAnalysisDef(name=analysis_name, arn=analysis_arn,QSAdminRegion=qs_admin_region, QSRegion=analysis_region, QSUser=username, AccountId=FIRST_STAGE_ACCOUNT_ID, PipelineName=PIPELINE_NAME, 
                             AssociatedDashboardId=dashboardId)
    analysis.datasets = datasetsDefObjList

    #Now we need to tag RLS datasets to make sure they are not included in Analysis template definition

    for dataset_id in rls_dataset_ids:
        rls_dset_obj = analysis.getDatasetById(dataset_id)
        rls_dset_obj.isRLS = True

    return analysis, ds_count

def lambda_handler(event, context):

    calledViaEB = False

    remap = REMAP_DS == 'true'
    generate_nested_stacks = GENERATE_NESTED_STACKS == 'true'

    print("Execution MODE is {mode}".format(mode=MODE))

    replication_handler = None
    credentials = assumeRoleInDeplAccount(role_arn=DEPLOYMENT_DEV_ACCOUNT_ROLE_ARN)        

    asset_id_list = read_all_assetIds_from_dynamo(region=AWS_REGION, credentials=credentials)

    # Validate if each asset on the list is actually a Dashboard
    for asset_id in asset_id_list:
        if not validate_asset_id(assetId=asset_id, region=AWS_REGION):
            return {
                'statusCode': 500,
                'body': 'Asset id {asset_id} is not a dashboard, at the moment only QuickSight dashboards are supported in this pipeline, please fix this and retry ...'.format(asset_id=asset_id)
            }

    analysisObjList = []

    source_account_yaml = {}
    dest_account_yaml = {}
    ds_index = 0

    # Now we are sure that all the assets on the list are dashboards, we can create a list of QSAnalysisDef objects with each of their originating analyses.
    for dashboardId in asset_id_list:
        analysisObj, ds_index = getAnalysisAssociatedWithDashboard(dashboardId=dashboardId, ds_index=ds_index)
        analysisObjList.append(analysisObj)        
    
    if 'source' in event and event['source'] == 'aws.quicksight':
        print('Lambda function called via EventBridge')
        calledViaEB = True
        if 'resources' in event:
            updated_dashboard_id = event['resources'].pop().split('dashboard/')[1]
        
            if updated_dashboard_id not in asset_id_list:
                print('This lambda is configured to promote dashboards configured in the DDB table {table_name} whose ids are {dashboard_ids}, however the updated dashboard in event is {updated_dashboard_id}. Skipping ...'
                    .format(table_name=TRACKED_ASSETS_TABLE_NAME, dashboard_ids=asset_id_list, updated_dashboard_id=updated_dashboard_id))
                return {
                    'statusCode': 200
                }
    
    if REPLICATION_METHOD == 'TEMPLATE':
        replication_handler = replicate_dashboard_via_template
    elif REPLICATION_METHOD == 'ASSETS_AS_BUNDLE':
        replication_handler = replicate_dashboard_via_AAB
    
    source_account_yaml, dest_account_yaml = replication_handler(analysisObjList, remap)

    if REPLICATION_METHOD == 'ASSETS_AS_BUNDLE':
        dest_account_yaml = add_permissions_to_AAB_resources(dest_account_yaml)

    try:
        QSSourceAssetsFilename = '{output_dir}/QS_assets_CFN_SOURCE.yaml'.format(output_dir=OUTPUT_DIR)
        writeToFile(filename=QSSourceAssetsFilename, content=source_account_yaml)
        
        QSDestAssetsFilename = '{output_dir}/QS_assets_CFN_DEST.yaml'.format(output_dir=OUTPUT_DIR)

        writeToFile(filename=QSDestAssetsFilename, content=dest_account_yaml)

        source_param_list = generate_cloudformation_template_parameters(template_content=source_account_yaml)
        dest_param_list = generate_cloudformation_template_parameters(template_content=dest_account_yaml)

        deployment_stages = STAGES_NAMES.split(",")[1:]

        if MODE == 'INITIALIZE':
            
            print("{mode} was requested, generating sample configuration files in {config_files_prefix} prefix on {bucket} in the deployment account {deployment_account} to be filled with \
                parametrized values for each environment".format(mode=MODE, config_files_prefix=CONFIGURATION_FILES_PREFIX, bucket=DEPLOYMENT_S3_BUCKET, deployment_account=DEPLOYMENT_ACCOUNT_ID))
            
            source_param_help = summarize_template(template_content=source_account_yaml, templateName="SourceAssets", s3Credentials=credentials, conf_files_prefix=CONFIGURATION_FILES_PREFIX)
            dest_param_help = summarize_template(template_content=dest_account_yaml, templateName="DestinationAssets", s3Credentials=credentials, conf_files_prefix=CONFIGURATION_FILES_PREFIX)            
            
            for stage in deployment_stages:
                source_assets_param_file_path = writeToFile('{output_dir}/source_cfn_template_parameters_{stage}.txt'.format(output_dir=OUTPUT_DIR, stage=stage.strip()), content=source_param_list, format='json')
                uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=source_assets_param_file_path, prefix=CONFIGURATION_FILES_PREFIX, region=DEPLOYMENT_S3_REGION, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=credentials)
                dest_assets_param_file_path = writeToFile('{output_dir}/dest_cfn_template_parameters_{stage}.txt'.format(output_dir=OUTPUT_DIR, stage=stage.strip()), content=dest_param_list, format='json')
                uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=dest_assets_param_file_path, prefix=CONFIGURATION_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)

                # Store parameter definition initialization for each stage in DDB tables
                #source Params
                store_dashboard_parameter_definition_in_dynamo(table_name=PARAMETER_DEFINITION_TABLE_NAME, assetType="source", stage=stage.strip(),  parameter_definition=json.dumps(source_param_list, indent=2),
                                                               parameter_help=json.dumps(source_param_help, indent=2), region=AWS_REGION, credentials=credentials)
                #dest Params
                store_dashboard_parameter_definition_in_dynamo(table_name=PARAMETER_DEFINITION_TABLE_NAME, assetType="dest", stage=stage.strip(), parameter_definition=json.dumps(dest_param_list, indent=2),
                                                               parameter_help=json.dumps(dest_param_help, indent=2), region=AWS_REGION, credentials=credentials)


        elif calledViaEB or (MODE == 'DEPLOY'):
            try:
                check_parameters_cloudformation(template_param_list=source_param_list, region=AWS_REGION, credentials=credentials, assetType="source")

                check_parameters_cloudformation(template_param_list=dest_param_list, region=AWS_REGION, credentials=credentials, assetType="dest")
            except ValueError as error:            
                print('There was an issue with the CFN parameters file for stage, correct your CFN parameter file or run the function again with MODE: ''INITIALIZE''')
                raise ValueError(error)
                

            print("{mode} was requested via event in Lambda, proceeding with the generation of assets based with the config  files in {config_files_prefix}\
                    prefix on {bucket} in the deployment account {deployment_account}".format(mode=MODE, config_files_prefix=ASSETS_FILES_PREFIX, bucket=DEPLOYMENT_S3_BUCKET, deployment_account=DEPLOYMENT_ACCOUNT_ID))
            
            # Create source artifact file
            zip_file = '{output_dir}/SOURCE_assets_CFN.zip'.format(output_dir=OUTPUT_DIR)

            source_files = get_s3_objects(bucket=DEPLOYMENT_S3_BUCKET, prefix='{config_files_prefix}/source_cfn_template_parameters_'.format(config_files_prefix=CONFIGURATION_FILES_PREFIX), region=DEPLOYMENT_S3_REGION, credentials=credentials)
            source_files.append(QSSourceAssetsFilename)

            ret_source = zipAndUploadToS3(bucket=DEPLOYMENT_S3_BUCKET, files=source_files, zip_name=zip_file,  prefix=ASSETS_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)

            # Create dest artifact file
            zip_file = '{output_dir}/DEST_assets_CFN.zip'.format(output_dir=OUTPUT_DIR)

            if generate_nested_stacks:
                if REPLICATION_METHOD == 'ASSETS_AS_BUNDLE':
                    resource_id_mapping = generate_resource_id_mapping(template_content=dest_account_yaml)
                    updated_dest_account_yaml = change_stack_references_to_ids(template_content=dest_account_yaml, resource_id_mapping=resource_id_mapping)
                    grouped_resources_content, grouped_parameters_content = split_stack_resources_and_parameters_into_groups(updated_dest_account_yaml)
                else:
                    grouped_resources_content, grouped_parameters_content = split_stack_resources_and_parameters_into_groups(dest_account_yaml)
                parent_dest_stack_yaml = generate_nested_stacks_from_grouped_resources(grouped_resources_content=grouped_resources_content, grouped_parameters_content=grouped_parameters_content, credentials=credentials)
                writeToFile(filename=QSDestAssetsFilename, content=parent_dest_stack_yaml)
            
            dest_files = get_s3_objects(bucket=DEPLOYMENT_S3_BUCKET, prefix='{config_files_prefix}/dest_cfn_template_parameters_'.format(config_files_prefix=CONFIGURATION_FILES_PREFIX), region=DEPLOYMENT_S3_REGION, credentials=credentials)
            dest_files.append(QSDestAssetsFilename)
            ret_dest = zipAndUploadToS3(bucket=DEPLOYMENT_S3_BUCKET, files=dest_files, zip_name=zip_file,  prefix=ASSETS_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)
    
    except ValueError as error:
        return {
            'statusCode': 500,
            'error': str(error)
        }
    
    return {
            'statusCode': 200
    }
    
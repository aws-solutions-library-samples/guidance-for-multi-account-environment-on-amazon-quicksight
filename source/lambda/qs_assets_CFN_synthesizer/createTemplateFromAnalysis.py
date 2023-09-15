import logging
import yaml
import json
import copy
from zipfile import ZipFile
import os
import boto3
import botocore
from botocore.exceptions import ClientError
from helpers.datasets import QSDataSetDef
from helpers.analysis import QSAnalysisDef
from helpers.datasources import SourceType, QSDataSourceDef, QSServiceDatasourceDef, QSRDSDatasourceDef, QSRDBMSDatasourceDef
from helpers.datasets import ImportMode
from datetime import datetime
from dateutil.relativedelta import relativedelta

now = datetime.now()

SOURCE_AWS_ACCOUNT_ID = os.environ['SOURCE_AWS_ACCOUNT_ID']
DEPLOYMENT_ACCOUNT_ID = os.environ['DEPLOYMENT_ACCOUNT_ID']
SOURCE_TEMPLATE_ID = ''
AWS_REGION = os.environ['AWS_REGION']
DEPLOYMENT_S3_BUCKET = os.environ['DEPLOYMENT_S3_BUCKET']
DEPLOYMENT_S3_REGION = os.environ['DEPLOYMENT_S3_REGION']
ASSUME_ROLE_EXT_ID = os.environ['ASSUME_ROLE_EXT_ID']
ANALYSIS_ID = ''
DASHBOARD_ID = ''
SOURCE_QS_USER = os.environ['SOURCE_QS_USER']
DEST_QS_USER = os.environ['DEST_QS_USER']
STAGES_NAMES = os.environ['STAGES_NAMES']

DEPLOYMENT_DEV_ACCOUNT_ROLE_ARN = 'arn:aws:iam::{deployment_account_id}:role/DevAccountS3AccessRole'.format(deployment_account_id=DEPLOYMENT_ACCOUNT_ID)
OUTPUT_DIR = '/tmp/output/'


try:
    os.mkdir(OUTPUT_DIR)
except FileExistsError:
    print('Output dir {output_dir} already exists, skipping'.format(output_dir=OUTPUT_DIR))


qs = boto3.client('quicksight', region_name=AWS_REGION)


def generateQSTemplateCFN(analysisName: str, datasetDict: dict, analysisArn: str):
    """Function that generates a Cloudformation AWS::QuickSight::Template resource https://a.co/7A8bfh7
    synthesized from a given analysisName

    Parameters:
    analysisName (str): Analysis name that will be templated
    datasetDict (dict): Dictionary of dataset names and placeholders
    analysisArn (str): Analysis ARN of the analysis that will be templated

    
    Returns:
    dict: yaml_template Object that represents the synthesized CFN template 
    str: templateId String that represents the templateId of the synthesized template

    Example:
    >>> generateQSTemplateCFN('Analysis Name', {'Dataset1': 'DatasetPlaceholder1', 'Dataset2': 'DatasetPlaceholder2'}, 'Analysis ARN')

    """
   
    with open('resources/template_CFN_skel.yaml', 'r') as file:
        yaml_template = yaml.safe_load(file)

    template_properties = yaml_template['Resources']['CICDQSTemplate']['Properties']
    analysis_name_sanitized = analysisName.replace(' ', '-')
    templateId = '{analysis_name}-template'.format(analysis_name=analysis_name_sanitized)
    analysis_id = analysisArn.split('/')[-1]

    # properties in template


    template_properties['SourceEntity']['SourceAnalysis']['Arn']['Fn::Sub'] = template_properties['SourceEntity']['SourceAnalysis']['Arn']['Fn::Sub'].replace('{analysis_id}', analysis_id)
    template_properties['TemplateId'] = templateId
    template_properties['Name'] = 'CI CD Template for analysis {name}'.format(name=analysisName)    

    # set up dataset references


    dataset_ref_list = []

    for datasetId in datasetDict:
        dataset = {}   
        datasetArnSubStr = 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:dataset/{dataset_id}'.replace('{dataset_id}', datasetId)

        dataset['DataSetArn'] = {}
        dataset['DataSetArn']['Fn::Sub'] = datasetArnSubStr        
        dataset['DataSetPlaceholder'] = datasetDict[datasetId].placeholdername
        dataset_ref_list.append(dataset)

    template_properties['SourceEntity']['SourceAnalysis']['DataSetReferences'] = dataset_ref_list

    return yaml_template, templateId
        
def generateDataSourceCFN(datasourceId: str, appendContent: dict, index: int, lambdaEvent: dict):
    """
    Function that generates a Cloudformation AWS::QuickSight::DataSource resource https://a.co/2xRL70Q
    synthesized from the source environment account

    Parameters:
    datasourceId (str): Datasource ID of the datasource that will be templated
    appendContent (dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements
    index (int): Index of the datasource in the list of datasources that will be templated    
    lambdaEvent (dict): Lambda event that contains optional parameters that alter the CFN resource that will be created, for example the REMAP_DS that, if provided \
                        will generate a parametrized CFN template to replace datasource parameters

    
    Returns:
    dict: appendContent Object that represents the synthesized CFN template 
    object: dataSourceDefObj helper datasource object representing the datasource object

   
    """

    originalAppendContent = copy.deepcopy(appendContent)
    updateTemplate=True
    

    ret = qs.describe_data_source(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSourceId=datasourceId)
    datasourceName = ret['DataSource']['Name']
    properties = {}
    RDMBS_DS = [SourceType.AURORA.name, SourceType.AURORA_POSTGRESQL.name,SourceType.MYSQL.name,SourceType.MARIADB.name,SourceType.ORACLE.name,SourceType.SQLSERVER.name, SourceType.REDSHIFT.name]
    SECRET_ONLY_DS = RDMBS_DS + [SourceType.REDSHIFT.name]
    
    if appendContent is None:
        print("Append content is None")
        raise ValueError("Error in createTemplateFromAnalysis:generateDataSourceCFN, Append content is None")
                
    with open('resources/datasource_CFN_skel.yaml', 'r') as file:
        yaml_datasource = yaml.safe_load(file)        
    id_sanitized = datasourceId.replace('-', '')
    datasourceIdKey = 'DS{id}'.format(id=id_sanitized)
    appendContent['Resources'][datasourceIdKey] = yaml_datasource
    properties = appendContent['Resources'][datasourceIdKey]['Properties']  

    if datasourceIdKey in originalAppendContent['Resources']:
        print('Datasource with CFNId {cfn_id} already exists, skipping'.format(cfn_id=datasourceIdKey)) 
        updateTemplate=False    

    properties['DataSourceId'] = datasourceId    
    properties['Name'] = datasourceName
    

    dsType = ret['DataSource']['Type']
    DSparameters ={}
    dataSourceDefObj = {}

    print("Processing datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index}".format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType))
    
    if dsType == SourceType.S3.name:
        destBucketKey = '{type}DestinationBucket{index}'.format(index=index, type=dsType)
        destKeyKey = '{type}DestinationKey{index}'.format(index=index, type=dsType)
        
        if 'REMAP_DS' in lambdaEvent:
            appendContent['Parameters'].update({
                destBucketKey: {
                    'Description' : 'S3 bucket to use for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                    'Type': 'String'
                },
                destKeyKey: {
                    'Description' : 'S3 key to use for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                    'Type': 'String'
                }
            })
            DSparameters['Bucket'] = {
                'Ref': destBucketKey
            }
            DSparameters['Key'] = {
                'Ref': destKeyKey
            }            
        else:
            DSparameters['Bucket'] = ret['DataSource']['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Bucket']
            DSparameters['Key'] = ret['DataSource']['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Key']

        dataSourceDefObj =  QSServiceDatasourceDef(name=datasourceName, id=datasourceId, parameters=DSparameters, type=SourceType.S3, index=index)
        properties['Type'] = 'S3'
        templateS3Parameters = {
            'S3Parameters': {
                'ManifestFileLocation': {}
            }
        }
        templateS3Parameters['S3Parameters']['ManifestFileLocation']['Bucket'] = dataSourceDefObj.parameters['Bucket']
        templateS3Parameters['S3Parameters']['ManifestFileLocation']['Key'] = dataSourceDefObj.parameters['Key']
        properties['DataSourceParameters'] = templateS3Parameters
    
    if dsType == SourceType.ATHENA.name:
        if 'REMAP_DS' in lambdaEvent:
            athenaWorkgroupKey = '{type}Workgroup{index}'.format(index=index, type=dsType)
            appendContent['Parameters'].update({
                athenaWorkgroupKey: {
                    'Description' : 'Athena Workgroup to use for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                    'Type': 'String'
                }
            })
            DSparameters['WorkGroup'] = {
                'Ref': athenaWorkgroupKey
            }
        else:
            DSparameters['WorkGroup'] = ret['DataSource']['DataSourceParameters']['AthenaParameters']['WorkGroup']
        dataSourceDefObj =  QSServiceDatasourceDef(name=datasourceName, id=datasourceId, parameters=DSparameters, type=SourceType.ATHENA, index=index)
        properties['Type'] = 'ATHENA'
        templateAthenaParameters = {
            'AthenaParameters': {}
        }
        templateAthenaParameters['AthenaParameters']['WorkGroup'] = dataSourceDefObj.parameters['WorkGroup']
        properties['DataSourceParameters'] = templateAthenaParameters

    if 'VpcConnectionProperties' in ret['DataSource']:
        vpcConnectionArn = ret['DataSource']['VpcConnectionProperties']['VpcConnectionArn']
        properties['VpcConnectionProperties'] = {
            'VpcConnectionArn': {
                'Ref': 'VpcConnectionArn'
            }
        }
        appendContent['Parameters'].update({
            'VpcConnectionArn':  {
                    'Description' : 'VPC Connection Arn to use in the stage, to be parametrized via CFN',
                    'Type': 'String'
                }
        }
        )

    if dsType in SECRET_ONLY_DS:
        if 'SecretArn' not in ret['DataSource']:
            raise ValueError("Datasource {datasource_name} (ID {datasource_id}) is a {type} datasource and it is not configured with a secret, cannot proceed".format(type=dsType, datasource_name=datasourceName, datasource_id=datasourceId))
        
        properties['Credentials'] = {
            'SecretArn':  {
                'Ref': 'DSSecretArn'
            }
        }
        appendContent['Parameters'].update({
            'DSSecretArn': {
                'Description' : 'Secret Arn to use in the stage, to be parametrized via CFN',
                'Type': 'String'                
            }
        })
    if dsType in RDMBS_DS:
        datasourceParametersKey = list(ret['DataSource']['DataSourceParameters'].keys()).pop()
        if 'RdsParameters' in ret['DataSource']['DataSourceParameters']:
            #its an RDS datasource
            rdsInstanceParam = 'RDSInstanceID{index}'.format(index=index)
            databaseParam = 'RDSDBName{index}'.format(index=index)
            if 'REMAP_DS' in lambdaEvent:
                appendContent['Parameters'].update({
                    rdsInstanceParam: {
                        'Description' : 'RDS Instance Id for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'String'
                    },
                    databaseParam: {
                        'Description' : 'Database name for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'String'
                    }
                })
                DSparameters['InstanceId'] = {
                    'Ref': rdsInstanceParam
                }
                DSparameters['Database'] = {
                    'Ref': databaseParam
                }

            else:
                DSparameters['InstanceId'] = ret['DataSource']['DataSourceParameters']['RdsParameters']['InstanceId']
            dataSourceDefObj =  QSRDSDatasourceDef(name=datasourceName, id=datasourceId, parameters=DSparameters, vpcConnectionArn=vpcConnectionArn, index=index)
            templateDSParameters = {
                datasourceParametersKey : {
                    'Database': dataSourceDefObj.database,
                    'InstanceId': dataSourceDefObj.instanceId
                }
            }            
        else:
            if 'REMAP_DS' in lambdaEvent:
                
                databaseParam = '{type}DBName{index}'.format(index=index, type=dsType)
                portParam = '{type}Port{index}'.format(index=index, type=dsType)
                hostParam = '{type}Host{index}'.format(index=index,type=dsType)
                appendContent['Parameters'].update({             
                    databaseParam: {
                        'Description' : 'Database name for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'String'
                    },
                    portParam: {
                        'Description' : 'Database port for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'Number'
                    },
                    hostParam: {
                        'Description' : 'Database host for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline. This parameter was added because REMAP_DS parameter was set in the synthesizer lambda'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'String'
                    }
                })
                
                DSparameters['Database'] = {
                    'Ref': databaseParam
                }    
                DSparameters['Port'] = {
                    'Ref': portParam
                }
                DSparameters['Host'] = {
                    'Ref': hostParam
                }        
            else:
                DSparameters['Host'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Host']
                DSparameters['Port'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Port']
                DSparameters['Database'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['Database']            
            templateDSParameters = {
                datasourceParametersKey : {
                    'Database': DSparameters['Database'],
                    'Host': DSparameters['Host'],
                    'Port': DSparameters['Port']
                }
            }  
        
        if dsType == SourceType.REDSHIFT.name:
            if 'REMAP_DS' in lambdaEvent:
                RSclusterIdParam = '{type}ClusterId{index}'.format(index=index,type=dsType)
                appendContent['Parameters'].update({
                    RSclusterIdParam: {
                        'Description' : 'ClusterId for datasource {datasource_name} (ID {datasource_id}, type {type}) with index {index} to use in the stage, to be parametrized via CFN deploy action in codepipeline see https://a.co/2aOOOTA for more information about how to set it in Codepipeline'.format(datasource_name=datasourceName, index=index, datasource_id=datasourceId, type=dsType),
                        'Type': 'String'
                    }
                })
                DSparameters['ClusterId'] = {
                    'Ref': RSclusterIdParam
                }
            else:
                DSparameters['ClusterId'] = ret['DataSource']['DataSourceParameters'][datasourceParametersKey]['ClusterId']
            
            templateDSParameters[datasourceParametersKey]['ClusterId'] = DSparameters['ClusterId']
            
            
        dataSourceDefObj =  QSRDBMSDatasourceDef(name=datasourceName, id=datasourceId, parameters=DSparameters, vpcConnectionArn=vpcConnectionArn, index=index)
        properties['Type'] = dsType
        properties['DataSourceParameters'] = templateDSParameters

    if updateTemplate:
        return appendContent, dataSourceDefObj
    else:
        return originalAppendContent, dataSourceDefObj


def generateDataSetCFN(datasetObj: object, datasourceObjs: QSDataSourceDef, tableMap: object, appendContent: dict, datasourceOrd:int):
    """
    Function that generates a Cloudformation AWS::QuickSight::DataSet resource https://a.co/5EVM6yD
    synthesized from the source environment account

    Parameters:

    datasetObj(object): Dataset object from the source environment account
    datasourceObjs(list): List of QSDataSourceDef objects
    tableMap (dict): Dictionary of table names and corresponding physical table names
    appendContent(dict): Dictionary that represents the CFN template object (already built by other methods) where we want to append elements
    datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)

    Returns:

    appendContent(dict): Dictionary containing the definition of Cloudformation template elements
    datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)    

    Examples:

    >>> generateDataSetCFN(datasetObj=datasetObj, datasourceObjs=datasourceObjs, tableMap=tableMap, appendContent=appendContent)

    """
    
    OPTIONAL_PROPS = ['ColumnGroups', 'FieldFolders', 'RowLevelPermissionTagConfiguration', 'ColumnLevelPermissionRules', 'DataSetUsageConfiguration', 'DatasetParameters']
    
    dependingDSources = []
    datasetId = datasetObj.id
    ret = qs.describe_data_set(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=datasetId)

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
        dependingDSources.append(datasourceObj.CFNId)

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
    appendContent['Resources'][dataSetIdKey]['DependsOn'] = dependingDSources

    appendContent = generateRefreshSchedulesCFN(datasetObj=datasetObj, appendContent=appendContent)

    for property in OPTIONAL_PROPS:
        if property in ret['DataSet'] and bool(ret['DataSet'][property]):
            properties[property] = ret['DataSet'][property]        

    if 'RowLevelPermissionDataSet' in ret['DataSet'] and bool(ret['DataSet']['RowLevelPermissionDataSet']):
        # Dataset contains row level permission, we need to create depending resources        
        appendContent, datasourceOrd = generateRowLevelPermissionDataSetCFN(appendContent=appendContent, targetDatasetIdKey=dataSetIdKey, rlsDatasetDef=ret['DataSet']['RowLevelPermissionDataSet'], datasourceOrd=datasourceOrd)
        

    return appendContent, datasourceOrd

def generateRowLevelPermissionDataSetCFN( appendContent:dict, targetDatasetIdKey:str, rlsDatasetDef:dict, datasourceOrd:int):
    """ Helper function that generates the dataset and datasource used to implement the RLS of a source dataset

    Args:        
        appendContent (dict): Dictionary containing the definition of Cloudformation template elements
        targetDatasetIdKey (str): Dataset CFNId this RLS applies to
        rlsDatasetDef (dict): Object defining the RLS dataset to be appplied to the target dataset
        datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)


    Returns:
        appendContent (dict): Dictionary containing the definition of Cloudformation template elements including the ones processed by this function
        datasourceOrd(int): number of datasources that have been generated (used to build the parameters in cloudformation)
    """
    ret_refresh_schedules  = []
    rlsDatasetId = rlsDatasetDef['Arn'].split('dataset/')[-1]

    retRLSDSet = qs.describe_data_set(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=rlsDatasetId)
    
    if retRLSDSet['DataSet']['ImportMode'] == ImportMode.SPICE.name:
        importMode = ImportMode.SPICE
        ret_refresh_schedules = qs.list_refresh_schedules(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=rlsDatasetId)
    else:
        importMode = ImportMode.DIRECT_QUERY

    tableKey = list(retRLSDSet['DataSet']['PhysicalTableMap'].keys()).pop()
    # Dynamically get the child key of PhysicalTableMap's table that could be either RelationalTable, CustomSql, S3Source as per https://shorturl.at/uP124
    tableChildKey = list(retRLSDSet['DataSet']['PhysicalTableMap'][tableKey].keys()).pop()
    rlsDatasourceArn = retRLSDSet['DataSet']['PhysicalTableMap'][tableKey][tableChildKey]['DataSourceArn'] 
    rlsDatasourceId = rlsDatasourceArn.split('/')[-1]    
    appendContent, RLSdataSourceDefObj = generateDataSourceCFN(datasourceId=rlsDatasourceId, appendContent=appendContent, index=datasourceOrd, lambdaEvent={'REMAP_DS':True})
    physicalTableKeys= get_physical_table_map_object(retRLSDSet['DataSet']['PhysicalTableMap'])
    RLSdatasetObj = QSDataSetDef(id=rlsDatasetId, name=retRLSDSet['DataSet']['Name'], importMode=importMode,physicalTableMap=physicalTableKeys, placeholdername=retRLSDSet['DataSet']['Name'], refreshSchedules=ret_refresh_schedules)
    RLSdatasetObj.dependingDSources = [RLSdataSourceDefObj]
    appendContent, datasourceOrd = generateDataSetCFN(datasetObj=RLSdatasetObj, datasourceObjs=RLSdatasetObj.dependingDSources, tableMap=RLSdatasetObj.physicalTableMap, appendContent=appendContent, datasourceOrd=datasourceOrd)    
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

    with open('resources/analysis_resource_CFN_skel.yaml', 'r') as file:
        yaml_analysis = yaml.safe_load(file)  

    properties = yaml_analysis['Properties']
    properties['AnalysisId'] = analysisObj.id
    properties['Name'] = analysisObj.name    

    sourceTemplateArnJoinObj = {
            'Fn::Sub': 'arn:aws:quicksight:${AWS::Region}:${SourceAccountID}:template/{template_id}'.replace('{template_id}', templateId)
    }

    properties['SourceEntity']['SourceTemplate']['Arn'] = sourceTemplateArnJoinObj
    datasets = analysisObj.datasets
    datasetReferencesObjList = []
    for datasetId in datasets:
        datasetReferencesObj = {}
        datasetObj = analysisObj.datasets[datasetId]
        datasetArnJoinObj = {
            'Fn::Sub': 'arn:aws:quicksight:${AWS::Region}:${AWS::AccountId}:dataset/{dataset_id}'.replace('{dataset_id}', datasetId)
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

        ret = qs.list_refresh_schedules(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=datasetObj.id)
        now = datetime.now()

        for schedule in ret['RefreshSchedules']:
            refresh_schedule_id = schedule['ScheduleId']
            retSchedule = qs.describe_refresh_schedule(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=datasetObj.id, ScheduleId=refresh_schedule_id)
            with open('resources/dataset_refresh_schedule_CFN_skel.yaml', 'r') as file:
                yaml_schedule = yaml.safe_load(file)  
            
            yaml_schedule['Properties']['DataSetId'] = datasetObj.id
            yaml_schedule['Properties']['Schedule'] = retSchedule['RefreshSchedule']
            # There is an inconsistency on the describe refresh API and Timezone is not Capitalized so we need this workaround to fix it.
            yaml_schedule['Properties']['Schedule']['ScheduleFrequency']['TimeZone'] = yaml_schedule['Properties']['Schedule']['ScheduleFrequency'].pop('Timezone')
            scheduleFrequency = retSchedule['RefreshSchedule']['ScheduleFrequency']['Interval']
            if scheduleFrequency == 'MONTHLY':
                futurestartAfterTimeTz = now + relativedelta(months=+1)
            elif scheduleFrequency == 'WEEKLY':
                futurestartAfterTimeTz = now + relativedelta(weeks=+1)
            else:
                futurestartAfterTimeTz = now + relativedelta(days=+7)
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
    except botocore.exceptions.ClientError as error:
        print('The provided bucket doesn\'t belong to the expected account {account_id}'.format(account_id=bucket_owner))
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
    
    with open(filename, '+w') as file:
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

    bool: True if execution was successful

    Examples:

    >>> summarize_template(template_content=template_content)

    """    
    DIVIDER_SECTION = "----------------------------------------------------------\n"
    parameters = template_content['Parameters']

    paramFilename = '{output_dir}/{template_name}_README.json'.format(output_dir=OUTPUT_DIR, template_name=templateName)
    
    writeToFile(content=parameters, filename=paramFilename, format="json")
    uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=paramFilename, region=AWS_REGION, object_name='{template_name}_README.json'.format(template_name=templateName), prefix=conf_files_prefix, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=s3Credentials)

    print(DIVIDER_SECTION)
    print("Template {template_name} contains parameters that need to be set in CodePipeline's CloudFormation artifact via file. This file has been uploaded to {file_location}, each development stage will have its own pair of parametrization files (source and dest).\
           You will need to download this file, edit the parameters according to your environments ({environments}) and then execute this function with \"MODE\" : \"DEPLOY\" key present in the lambda event. Refer to https://a.co/0DrKhVm for more \
           information on how to use this file".format(template_name=templateName, file_location=conf_files_prefix, environments=parameters.keys()))
    print("Find below a lisf of the parameters needed, also README.json files for each stack (source and dest) have been created on the aforementioned S3 bucket and prefix:")
    print("")
    for parameter in parameters.keys():
        print("{parameter}: {description}".format(parameter=parameter, description=parameters[parameter]['Description']))

    param_formatted = [ 'ParameterKey={parameter},ParameterValue='.format(parameter=parameter)  for parameter in parameters.keys() ]
    print("")
    print("Remember that you can still define parameter override values in CodePipeline deploy actions following this format, however for scalability configuration files are recommended to be used instead:")
    print('\n'.join(param_formatted))
    print(DIVIDER_SECTION)

    return True

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

def lambda_handler(event, context):
    ds_count=0
    CONFIGURATION_FILES_PREFIX = '{pipeline_name}/ConfigFiles'
    ASSETS_FILES_PREFIX = '{pipeline_name}/CFNTemplates'

    if (event is not None and 'DASHBOARD_ID' in event ):
        DASHBOARD_ID = event['DASHBOARD_ID']
    else:
        raise ValueError('No dashboard id provided in lambda event object')
    
    # Get the associated analyisis

    ret = qs.describe_dashboard(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DashboardId=DASHBOARD_ID)

    source_analysis_arn = ret['Dashboard']['Version']['SourceEntityArn']

    ANALYSIS_ID = source_analysis_arn.split('analysis/')[1]

    ret = qs.describe_analysis(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, AnalysisId=ANALYSIS_ID)
    permissions = qs.describe_analysis_permissions(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, AnalysisId=ANALYSIS_ID)
    owner = permissions['Permissions'].pop()
    username =  owner['Principal'].split('default/')
    qs_admin_region = owner['Principal'].split(':')[3]

    analysis_arn = ret['Analysis']['Arn']
    analysis_region = analysis_arn.split(':')[3]
    analysis_name = ret['Analysis']['Name']
    analysis_id = ret['Analysis']['AnalysisId']
    dataset_arns = ret['Analysis']['DataSetArns']

    datasources = []
    datasourceDefObjList = []
    datasetsDefObjList = []
    datasetDictionary = {}
    dest_account_yaml = {}

    with open('resources/dest_CFN_skel.yaml', 'r') as file:
        dest_account_yaml = yaml.safe_load(file)     

    dest_account_yaml['Resources'] = {} 

    physicalTableKeys = []




    for datasetarn in dataset_arns:
        physicalTableKeys = []
        datasourceDefObjList = []
        datasources = []
        datasetId = datasetarn.split('dataset/')[-1]
        ret = qs.describe_data_set(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=datasetId )
        
        datasourceArn = ''
        physicalTableKeys= get_physical_table_map_object(ret['DataSet']['PhysicalTableMap'])
        
        importMode = None
        ret_refresh_schedules  = []
        if ret['DataSet']['ImportMode'] == ImportMode.SPICE.name:
            importMode = ImportMode.SPICE
            ret_refresh_schedules = qs.list_refresh_schedules(AwsAccountId=SOURCE_AWS_ACCOUNT_ID, DataSetId=datasetId)
        else:
            importMode = ImportMode.DIRECT_QUERY



        datasetObj = QSDataSetDef(name=ret['DataSet']['Name'], id=datasetId, importMode=importMode, placeholdername=ret['DataSet']['Name'], refreshSchedules=ret_refresh_schedules, physicalTableMap=physicalTableKeys)
        datasetDictionary[datasetId] = datasetObj    

        for physicalTableKey in physicalTableKeys:
            tableTypeKey = list(ret['DataSet']['PhysicalTableMap'][physicalTableKey].keys()).pop()
            datasourceArn = ret['DataSet']['PhysicalTableMap'][physicalTableKey][tableTypeKey]['DataSourceArn']
            datasourceId = datasourceArn.split('datasource/')[-1]
            datasources.append(datasourceId)
        
        #Using set to avoid duplicated datasources to be created (one dataset could use the same datasource several times)
        datasources = list(set(datasources))

        dataSourceDefObj = {}
        for datasourceId in datasources:
            try:
                exists = False
                CFNId = 'DS{id}'.format(id=datasourceId.replace('-', ''))
                if CFNId in dest_account_yaml['Resources']:
                    exists = True                    
                dest_account_yaml, dataSourceDefObj = generateDataSourceCFN(datasourceId=datasourceId, appendContent=dest_account_yaml, index=ds_count, lambdaEvent=event)
                datasourceDefObjList.append(dataSourceDefObj)
            except ValueError as error:
                print(error)
                print('There was an issue creating the following datasource: {datasourceId} cannot proceed further'.format(datasourceId=datasourceId))
                return {
                'statusCode': 500
                }
            
            if not exists:
                ds_count = ds_count + 1           

        datasetObj.dependingDSources = datasourceDefObjList

    datasetsDefObjList.append(datasetObj)

    source_account_yaml, SOURCE_TEMPLATE_ID = generateQSTemplateCFN(analysisName=analysis_name, datasetDict=datasetDictionary, analysisArn=analysis_arn)
    analysis = QSAnalysisDef(name=analysis_name, id=analysis_id,QSAdminRegion=qs_admin_region, QSRegion=analysis_region, QSUser=username, AccountId=SOURCE_AWS_ACCOUNT_ID, TemplateId=SOURCE_TEMPLATE_ID)
    analysis.datasets = datasetDictionary

    source_account_yaml, dest_account_yaml = generate_template_outputs(analysis_obj=analysis, source_template_content=source_account_yaml, dest_template_content=dest_account_yaml)

    QSSourceAssetsFilename = '{output_dir}/QStemplate_CFN_SOURCE.yaml'.format(output_dir=OUTPUT_DIR)
    writeToFile(filename=QSSourceAssetsFilename, content=source_account_yaml)

    datasourceOrd = ds_count
    for datasetId in analysis.datasets.keys():        
        datasetObj = analysis.datasets[datasetId]    
        dest_account_yaml, datasourceOrd = generateDataSetCFN(datasetObj=datasetObj, datasourceObjs=datasetObj.dependingDSources, tableMap=datasetObj.physicalTableMap, appendContent=dest_account_yaml, datasourceOrd=datasourceOrd)


    dest_account_yaml = generateAnalysisFromTemplateCFN(analysisObj=analysis, templateId=SOURCE_TEMPLATE_ID, appendContent=dest_account_yaml)

    QSDestAssetsFilename = '{output_dir}/QS_assets_CFN_DEST.yaml'.format(output_dir=OUTPUT_DIR)

    writeToFile(filename=QSDestAssetsFilename, content=dest_account_yaml)

    # Upload assets to S3 in deployment account

    credentials = assumeRoleInDeplAccount(role_arn=DEPLOYMENT_DEV_ACCOUNT_ROLE_ARN)

    if 'PIPELINE_NAME' in event:
        CONFIGURATION_FILES_PREFIX = CONFIGURATION_FILES_PREFIX.format(pipeline_name=event['PIPELINE_NAME'])
        ASSETS_FILES_PREFIX = ASSETS_FILES_PREFIX.format(pipeline_name=event['PIPELINE_NAME'])
    else:
        CONFIGURATION_FILES_PREFIX = 'ConfigFiles/'
        ASSETS_FILES_PREFIX = 'CFNTemplates'

    if 'MODE' not in event or event['MODE'] == 'INITIALIZE':
        
        print("{mode} was requested via event in Lambda, generating sample configuration files in {config_files_prefix} prefix on {bucket} in the deployment account {deployment_account} to be filled with \
            parametrized values for each environment".format(mode=event['MODE'], config_files_prefix=CONFIGURATION_FILES_PREFIX, bucket=DEPLOYMENT_S3_BUCKET, deployment_account=DEPLOYMENT_ACCOUNT_ID))

        for stage in STAGES_NAMES.split(",")[1:]:
            source_param_list = generate_cloudformation_template_parameters(template_content=source_account_yaml)
            dest_param_list = generate_cloudformation_template_parameters(template_content=dest_account_yaml)
            source_assets_param_file_path = writeToFile('{output_dir}/source_cfn_template_parameters_{stage}.txt'.format(output_dir=OUTPUT_DIR, stage=stage.strip()), content=source_param_list, format='json')
            uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=source_assets_param_file_path, prefix=CONFIGURATION_FILES_PREFIX, region=DEPLOYMENT_S3_REGION, bucket_owner=DEPLOYMENT_ACCOUNT_ID, credentials=credentials)
            dest_assets_param_file_path = writeToFile('{output_dir}/dest_cfn_template_parameters_{stage}.txt'.format(output_dir=OUTPUT_DIR, stage=stage.strip()), content=dest_param_list, format='json')
            uploadFileToS3(bucket=DEPLOYMENT_S3_BUCKET, filename=dest_assets_param_file_path, prefix=CONFIGURATION_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)

        summarize_template(template_content=source_account_yaml, templateName="SourceAssets", s3Credentials=credentials, conf_files_prefix=CONFIGURATION_FILES_PREFIX)
        summarize_template(template_content=dest_account_yaml, templateName="DestinationAssets", s3Credentials=credentials, conf_files_prefix=CONFIGURATION_FILES_PREFIX)

    elif event['MODE'] == 'DEPLOY':

        print("{mode} was requested via event in Lambda, proceeding with the generation of assets based with the config  files in {config_files_prefix}\
            prefix on {bucket} in the deployment account {deployment_account}".format(mode=event['MODE'], config_files_prefix=ASSETS_FILES_PREFIX, bucket=DEPLOYMENT_S3_BUCKET, deployment_account=DEPLOYMENT_ACCOUNT_ID))

        # Create source artifact file
        zip_file = '{output_dir}/SOURCE_assets_CFN.zip'.format(output_dir=OUTPUT_DIR)
        
        source_files = get_s3_objects(bucket=DEPLOYMENT_S3_BUCKET, prefix='{config_files_prefix}/source_cfn_template_parameters_'.format(config_files_prefix=CONFIGURATION_FILES_PREFIX), region=DEPLOYMENT_S3_REGION, credentials=credentials)
        source_files.append(QSSourceAssetsFilename)

        ret_source = zipAndUploadToS3(bucket=DEPLOYMENT_S3_BUCKET, files=source_files, zip_name=zip_file,  prefix=ASSETS_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)

        # Create dest artifact file
        zip_file = '{output_dir}/DEST_assets_CFN.zip'.format(output_dir=OUTPUT_DIR)
        
        dest_files = get_s3_objects(bucket=DEPLOYMENT_S3_BUCKET, prefix='{config_files_prefix}/dest_cfn_template_parameters_'.format(config_files_prefix=CONFIGURATION_FILES_PREFIX), region=DEPLOYMENT_S3_REGION, credentials=credentials)
        dest_files.append(QSDestAssetsFilename)
        ret_dest = zipAndUploadToS3(bucket=DEPLOYMENT_S3_BUCKET, files=dest_files, zip_name=zip_file,  prefix=ASSETS_FILES_PREFIX, bucket_owner=DEPLOYMENT_ACCOUNT_ID, region=DEPLOYMENT_S3_REGION, credentials=credentials)


    return {
        'statusCode': 200
    }
    
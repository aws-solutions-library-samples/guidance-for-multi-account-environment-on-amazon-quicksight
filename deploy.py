import boto3
from botocore.exceptions import ClientError
import logging
import yaml
import argparse
import shutil
import os
import sys


def uploadFileToS3(bucket: str, filename: str, region: str, prefix=None, object_name=None):

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

    
    s3 = boto3.client('s3', region_name=region)     
        
    if prefix is not None:
        if prefix[-1] != '/':
            object_name = '{prefix}/{object}'.format(prefix=prefix, object=object_name)
        else:
            object_name = '{prefix}{object}'.format(prefix=prefix, object=object_name)
    
    # Upload the file
    
    try:
        response = s3.upload_file(filename, bucket, object_name,  ExtraArgs={
        'GrantRead': 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'
    })
        print('File {file} uploaded successfully to {bucket} at prefix {prefix}'.format(file=object_name, bucket=bucket, prefix=prefix))
    except ClientError as e:
        logging.error(e)
        print('There was an error uploading file {file} to {bucket} at prefix {prefix}'.format(file=object_name, bucket=bucket, prefix=prefix))
        return False
    return True

def print_yellow(text): print("\033[93m {}\033[00m" .format(text))
def print_green(text): print("\033[92m {}\033[00m" .format(text))
def print_red(text): print("\033[91m {}\033[00m" .format(text))

def check_bucket_public_block_acls(bucket_name, region):
    s3 = boto3.client('s3', region_name=region)
    s3control = boto3.client('s3control', region_name=region)
    try:
        controlresponse = s3control.get_public_access_block(
            AccountId=param_bucket_account_id
        )
        response = s3.get_public_access_block(Bucket=bucket_name)
        if controlresponse['PublicAccessBlockConfiguration']['BlockPublicAcls'] is True:
            print_red('ERROR: The AWS Account with id {account_id} has have public access block configured for all buckets, so the script cannot proceed, change this configuration and try again'.format(account_id=param_bucket_account_id))            
            print_red('Change this setting in the S3 console https://{region}.console.aws.amazon.com/s3/settings'.format(region=region))
            return False
        if response['PublicAccessBlockConfiguration']['BlockPublicAcls'] is True:
            print_red('ERROR: The bucket {bucket} has have public access block configured, so the script cannot proceed, change this configuration and try again'.format(bucket=bucket_name))
            print_red('Change this setting in the S3 console https://{region}.console.aws.amazon.com/s3/buckets/{bucket_name}?bucketType=general&tab=permissions'.format(region=region, bucket_name=bucket_name))
            return False
        else:
            return True
    except ClientError as e:
        print("Error checking S3 Block Public Access configuration")
        return False
    

def validate_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket_website = bucket.Website()

    try:
        bucket_website.index_document
    except ClientError as e:
        print()
        print_yellow('WARNING: The bucket {bucket} does not have a website configured so template files won''t be uploaded to S3'.format(bucket=bucket_name))
        print()
        return False

    return True    



parser=argparse.ArgumentParser()
parser.add_argument("--bucket_account_id", required=True, help="Account ID owning the S3 bucket where the solution code and templates will be uploaded, you need to have valid IAM credentials to upload objects to it")
parser.add_argument("--bucket", required=True, help="S3 bucket where the solution code and templates will be uploaded, you need to have valid IAM credentials to upload objects to it")
parser.add_argument("--bucket_region", required=True, help="Region of S3 bucket where the solution code and templates will be uploaded")
parser.add_argument("--template_prefix",required=True, help="prefix within your S3 bucket where templates will be uploaded, you need to have valid IAM credentials to upload objects to it")
parser.add_argument("--code_prefix", required=True, help="prefix within your S3 bucket where the code will be uploaded, you need to have valid IAM credentials to upload objects to it")
parser.add_argument("--deployment_account_id", required=True, help="Account ID that you will be using as deployment account, needs to be the management account on the organization")
parser.add_argument("--development_account_id", required=True, help="Account ID that you will be using as development account. It can be any account within the same organization as the deployment account")
parser.add_argument("--prepro_account_id", required=True, help="Account ID that you will be using as pre-production account. It can be any account within the same organization as the deployment account")
parser.add_argument("--production_account_id", required=True, help="Account ID that you will be using as production account. It can be any account within the same organization as the deployment account")
parser.add_argument("--pipeline_name", help="Name of the pipeline that will be created, defaults to QSPipeline", default="QSPipeline")
parser.add_argument("--admin_role", help="Name of the pipeline that will be created, defaults to QSPipeline", default="Admin")
parser.add_argument("--approval_email", help="Email to send by the pipeline for approval step", default="user@domain.com")
args=parser.parse_args()

LOCAL_LAYER_PATH='source/lambda/layer/lambdaLayerBotoYAML.zip'
LOCAL_CODE_PATH='source/lambda/qs_assets_CFN_synthesizer'
ZIP_CODE_FILE='qs_assets_CFN_synthesizer'
FIRST_ACCOUNT_TEMPLATE_PATH='deployment/CFNStacks/firstStageAccount_template.yaml'
DEPLOYMENT_TEMPLATE_PATH='deployment/CFNStacks/deploymentAccount_template.yaml'
CFN_STACK_SET_EXECUTION_ROLE_NAME='deployment/CFNStacks/AWSCloudFormationStackSetExecutionRole.yml'
CFN_STACKS_ROLE_CREATION_TEMPLATE_PATH='deployment/CFNStacks/AWSCloudFormationStackSetExecutionRole.yml'
WORKSPACE_DIR='workspace'

first_account_template = yaml.safe_load(open(FIRST_ACCOUNT_TEMPLATE_PATH, 'r'))
deployment_account_template = yaml.safe_load(open(DEPLOYMENT_TEMPLATE_PATH, 'r'))
cfn_stack_role_creation_template = yaml.safe_load(open(CFN_STACKS_ROLE_CREATION_TEMPLATE_PATH, 'r'))



param_bucket = args.bucket
param_bucket_account_id = args.bucket_account_id
code_prefix = args.code_prefix
template_prefix = args.template_prefix
deployment_account_id = args.deployment_account_id
development_account_id = args.development_account_id  
preproduction_account_id = args.prepro_account_id
production_account_id = args.production_account_id
approval_email = args.approval_email
admin_role = args.admin_role
depl_admin_role_arn = 'arn:aws:iam::{account_id}:role/{role}'.format(account_id=deployment_account_id, role=admin_role)


first_account_template['Parameters']['SourceCodeS3Bucket']['Default'] = param_bucket
first_account_template['Parameters']['SourceCodeKey']['Default'] = '{code_prefix}/qs_assets_CFN_synthesizer.zip'.format(code_prefix=code_prefix)
first_account_template['Parameters']['LayerCodeKey']['Default'] = '{code_prefix}/lambdaLayerBotoYAML.zip'.format(code_prefix=code_prefix)
first_account_template['Parameters']['DeploymentAccountId']['Default'] = deployment_account_id
first_account_template['Parameters']['PipelineName']['Default'] = args.pipeline_name
first_account_template['Parameters']['DeploymentS3Bucket']['Default'] = 'qs-pipeline-bucket-{account_id}'.format(account_id=deployment_account_id)

deployment_account_template['Parameters']['DevelopmentAccountId']['Default'] = development_account_id
deployment_account_template['Parameters']['PreProdAccountId']['Default'] = preproduction_account_id
deployment_account_template['Parameters']['ProdAccountId']['Default'] = production_account_id
deployment_account_template['Parameters']['AccountAdminARN']['Default'] = depl_admin_role_arn
deployment_account_template['Parameters']['PipelineName']['Default'] = args.pipeline_name
deployment_account_template['Parameters']['ApprovalEmail']['Default'] = approval_email
deployment_account_template['Parameters']['PipelineS3BucketName']['Default'] = 'qs-pipeline-bucket-{account_id}'.format(account_id=deployment_account_id)

cfn_stack_role_creation_template['Parameters']['AdministratorAccountId']['Default'] = deployment_account_id

try: 
    shutil.rmtree(WORKSPACE_DIR)    
except OSError as e:
    print('Directory {dir_name} doesn''t exist, skipping deletion'.format(dir_name=WORKSPACE_DIR))

try:
    os.mkdir(WORKSPACE_DIR)
except FileExistsError as e:
    print('Directory {dir_name} already exists, skipping creation'.format(dir_name=WORKSPACE_DIR))

first_account_template_file = 'firstStageAccount_template_customized.yaml'
deployment_account_template_file = 'deploymentAccount_template_customized.yaml'
cfn_stack_role_creation_template_file = 'AWSCloudFormationStackSetExecutionRole_customized.yaml'

output_first_account_template_path = "{workspace_dir}/{file}".format(workspace_dir=WORKSPACE_DIR, file=first_account_template_file) 
output_deployment_account_template_path = "{workspace_dir}/{file}".format(workspace_dir=WORKSPACE_DIR, file=deployment_account_template_file)
output_stack_role_creation_template_path = "{workspace_dir}/{file}".format(workspace_dir=WORKSPACE_DIR, file=cfn_stack_role_creation_template_file)

# Write first account customized template
with open(output_first_account_template_path, 'w+') as f:
    yaml.dump(first_account_template, f)

# Write deployment account customized template
with open(output_deployment_account_template_path, 'w+') as f:
    yaml.dump(deployment_account_template, f)

# Write cfn stack role creation customized template
with open(output_stack_role_creation_template_path, 'w+') as f:
    yaml.dump(cfn_stack_role_creation_template, f)

# zip code
zip_code_output = "{workspace_dir}/{archive_file}".format(workspace_dir=WORKSPACE_DIR, archive_file=ZIP_CODE_FILE)
shutil.make_archive(base_name=zip_code_output, root_dir=LOCAL_CODE_PATH, format='zip')

if not check_bucket_public_block_acls(bucket_name=param_bucket, region=args.bucket_region):
    print_red('ERROR: Either AWS account id {aws_account_id} or bucket {bucket} has Block public access enabled and will not allow the code to be uploaded change it to proceed'.format(aws_account_id=param_bucket_account_id, bucket=param_bucket))
else:

    # upload layer
    uploadFileToS3(bucket=param_bucket, filename=LOCAL_LAYER_PATH,region=args.bucket_region, prefix=code_prefix, object_name='lambdaLayerBotoYAML.zip')

    # upload code
    uploadFileToS3(bucket=param_bucket, filename='{file_name}.zip'.format(file_name=zip_code_output),region=args.bucket_region, prefix=code_prefix, object_name='qs_assets_CFN_synthesizer.zip')


    #remove previous zipped files

    cfn_one_click_deployment_url =\
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/new?stackName=CICDDeployment{PipelineName}&templateURL=https://{bucket_name}.s3.amazonaws.com/{template_prefix}/{template_file}"\
        .format(region=args.bucket_region, PipelineName=args.pipeline_name, bucket_name=args.bucket, template_prefix=template_prefix, template_file=deployment_account_template_file)
    cfn_one_click_first_stage_url =\
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/new?stackName=CICDFirstStage{PipelineName}&templateURL=https://{bucket_name}.s3.amazonaws.com/{template_prefix}/{template_file}"\
            .format(region=args.bucket_region, PipelineName=args.pipeline_name, bucket_name=args.bucket, template_prefix=template_prefix, template_file=first_account_template_file)
    cfn_one_cfn_stack_role_creation_url =\
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/new?stackName=DelegatedAccessStacksets&templateURL=https://{bucket_name}.s3.amazonaws.com/{template_prefix}/{template_file}"\
            .format(region=args.bucket_region, bucket_name=args.bucket, template_prefix=template_prefix, template_file=cfn_stack_role_creation_template_file)

    print("Assets successfully uploaded to {bucket} bucket".format(bucket=param_bucket))


    if validate_bucket(bucket_name=param_bucket):

        # upload firstStageAccount_template
        uploadFileToS3(bucket=param_bucket, filename=output_first_account_template_path, region=args.bucket_region, prefix=template_prefix, object_name=first_account_template_file)

        # upload deployment_template
        uploadFileToS3(bucket=param_bucket, filename=output_deployment_account_template_path, region=args.bucket_region, prefix=template_prefix, object_name=deployment_account_template_file)

        # upload cfn stack role creation template
        uploadFileToS3(bucket=param_bucket, filename=output_stack_role_creation_template_path, region=args.bucket_region, prefix=template_prefix, object_name=cfn_stack_role_creation_template_file)

        print()
        print_green('NEXT STEPS')
        print('# 1. Deploy deployment account assets in Deployment account with ID {dep_account_id} using the following URL: {dep_dep_url}'.format(dep_account_id=deployment_account_id, dep_dep_url=cfn_one_click_deployment_url))
        print('# 2. Deploy first stage account assets in  Development account with ID {dev_account_id} using the following URL: {dev_dep_url}'.format(dev_account_id=development_account_id, dev_dep_url=cfn_one_click_first_stage_url))
        print('# 3 (optional). If your account is not configured for CFN Stack Sets operation in self managed mode, deploy CloudFormation StackSet role creation in each environment account ({dev_account_id}, {pre_account_id}, {pro_account_id}) using the following URL: {dev_dep_url}'\
            .format(dev_account_id=development_account_id, pre_account_id=preproduction_account_id, pro_account_id=production_account_id, dev_dep_url=cfn_one_cfn_stack_role_creation_url))
    else:
        print()
        print_yellow('MANUAL ACTION REQUIRED - read below')
        print('Bucket {bucket_name} doesn''t have static webhosting enabled which is required to deploy CloudFormation templates directly from S3'.format(bucket_name=param_bucket))
        print('Solution lambda code files have been uploaded to your bucket so they can be used in the templates, but you will need to manually upload them in the CloudFormation console, files can be found under the workspace/ directory.')
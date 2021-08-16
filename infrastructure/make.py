#!/usr/bin/env python3

import json
import os
import subprocess
import argparse
import jinja2
import hashlib
import re
import boto3

terminalcolors = lambda:None
terminalcolors.HEADER = '\033[95m'
terminalcolors.OKBLUE = '\033[94m'
terminalcolors.OKGREEN = '\033[92m'
terminalcolors.WARNING = '\033[93m'
terminalcolors.FAIL = '\033[91m'
terminalcolors.ENDC = '\033[0m'
terminalcolors.BOLD = '\033[1m'
terminalcolors.UNDERLINE = '\033[4m'

def retrieveParamValue(parameter_list, search_param):
    for param in parameter_list:
        if param['Name'].endswith(search_param):
            return param['Value']
    else:
        raise Exception('SSM Param could not be found!')

def templatize(template_file, data_dict, output_folder, output_file=None):
    '''
    renders the template and returns it fully processed
    '''
    print(terminalcolors.HEADER + 'Rendering template:' + terminalcolors.ENDC +'[%s] into [%s]' % (template_file, './'+output_folder+'/'+output_file))
    templateLoader = jinja2.FileSystemLoader(searchpath='./')
    templateEnv = jinja2.Environment(loader=templateLoader)
    template = templateEnv.get_template(template_file)
    text = template.render(**data_dict)  # this is where to put args to the template renderer
    if output_file is not None:
        output = open('./'+output_folder+'/'+output_file, 'w')
        output.write('%s' % text)
        output.close()
    return text

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', choices=['dev', 'dev-sca', 'stage', 'stage-sca', 'prod', 'prod-sca'], help='environment is optional to reduce processing time', required=True)
    parser.add_argument('--instance', help='application instance within the environment', default='1', required=False)
    return parser.parse_args()

if __name__=='__main__':
    args = parse_args()

    # Ensure valid instance (only dev environment may have instances greater than 1)
    valid_nonprod_instances = ['1', '2', '3']
    if args.environment != 'dev' and args.instance != '1':
        print(terminalcolors.FAIL + f'Invalid Instance ({args.instance}) provide.  Valid instances non-dev environments are: "1".' + terminalcolors.ENDC)
        os._exit(1)
    elif args.instance not in valid_nonprod_instances:
        print(terminalcolors.FAIL + f'Invalid Instance ({args.instance}) provide.  Valid instances are: {valid_nonprod_instances}.' + terminalcolors.ENDC)
        os._exit(1)

    # First instances aren't numbered
    if args.instance == '1':
        args.instance = '' 
    
    # Determine pipeline type based off of environment
    sca_environments = ['dev-sca', 'stage-sca', 'prod-sca']
    if args.environment in sca_environments:
        pipeline_type = 'sca'
    else:
        pipeline_type = 'firmware'


    # Retrieve metadata
    ssm_client = boto3.client('ssm')
    stage_account_id = ssm_client.get_parameter(
        Name='/org/member/finitestate_stage/account_id')['Parameter']['Value']

    prod_account_id = ssm_client.get_parameter(
        Name='/org/member/finitestate_prod/account_id')['Parameter']['Value']

    bucket_name = ssm_client.get_parameter(
        Name='/org/member/finitestate_tools/codepipeline_artifacts_bucket_name')['Parameter']['Value']

    codepipeline_kms_key_arn = ssm_client.get_parameter(
        Name='/org/member/finitestate_tools/codepipeline_artifacts_bucket_kms_key_arn')['Parameter']['Value']

    github_token_arn = ssm_client.get_parameter(
        Name='/org/member/finitestate/github_token_secret_arn')['Parameter']['Value']

    app_deployment_notifications_arn = ssm_client.get_parameter(
        Name='/org/member/finitestate-tools/notifications/app-deployment-notifications')['Parameter']['Value']

    app_operations_notifications_arn = ssm_client.get_parameter(
        Name='/org/member/finitestate-tools/notifications/app-operations-notifications')['Parameter']['Value']

    # Replace is neccesary for `stage-sca` and `prod-sca` environments
    parameter_list = ssm_client.get_parameters(
        Names= [
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/account_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/vpc_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/private_hosted_zone_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/public_subnet_1_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/public_subnet_2_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/public_subnet_3_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/private_subnet_1a_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/private_subnet_2a_id',
            f'/org/member/finitestate_{args.environment.replace("-", "_")}/private_subnet_3a_id',
        ]
    )['Parameters']


    # Create pipeline directory
    try:
        os.mkdir('./'+'resources')
    except OSError:
        True

    tpl_data = {
        'env':args.environment,
        'instance': args.instance,
        'pipeline_type': pipeline_type,
        'codepipeline_bucket_name': bucket_name,
        'codepipeline_kms_key_arn': codepipeline_kms_key_arn,
        'github_token_arn': github_token_arn,
        'target_account_id': retrieveParamValue(parameter_list, 'account_id'),
        'stage_account_id': stage_account_id,
        'prod_account_id': prod_account_id,
        'vpc_id': retrieveParamValue(parameter_list, 'vpc_id'),
        'public_subnets': ','.join([param['Value'] for param in parameter_list if 'public_subnet' in param['Name']]),
        'private_subnets': ','.join([param['Value'] for param in parameter_list if 'private_subnet' in param['Name']]),
        'app_deployment_notifications_arn': app_deployment_notifications_arn,
        'app_operations_notifications_arn': app_operations_notifications_arn,
        'private_hosted_zone_id': retrieveParamValue(parameter_list, 'private_hosted_zone_id')
    }

    # Conditionally add attributes based on environment
    if args.environment == 'dev':
        tpl_data['acm_cert_arn'] = 'arn:aws:acm:us-east-1:185231689230:certificate/a11659eb-36a4-4036-867c-b3e62a631716'
    elif args.environment == 'stage':
        tpl_data['acm_cert_arn'] = 'arn:aws:acm:us-east-1:403455490220:certificate/0bc82aec-f98b-4e4d-bd17-de7f2d7f390a'
    elif args.environment == 'prod':
        tpl_data['acm_cert_arn'] = 'arn:aws:acm:us-east-1:383877828724:certificate/2b8fb2f4-8251-4291-bd3f-ad3bd85f8bde'

    # # # render templates # # #

    # render SCA specific templates
    if pipeline_type == 'sca':
        pass # 'sca' specific templates would be placed here... 

    # render generic templates
    templatize('templates/pipeline.yaml', tpl_data, 'resources', 'pipeline.yaml')
    templatize('templates/pgsync.yaml', tpl_data, 'resources', 'pgsync.yaml')

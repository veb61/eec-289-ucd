import boto3
import json
from boto3_type_annotations.iam import ServiceResource, Client


def iam_client():
    iam: Client = boto3.client('iam')
    return iam


def iam_resource():
    iam: ServiceResource = boto3.resource('iam')
    return iam


def current_user_arn():
    return iam_resource().CurrentUser().arn


def create_user(username):
    iam_client().create_user(UserName=username)


def get_user(username):
    iam_client().get_user(UserName=username)


def list_users():
    return iam_client().list_users()


def create_group(group_name):
    return iam_client().create_group(GroupName=group_name)


def list_groups():
    return iam_client().list_groups()


def create_policy(policyname, policy_doc):
    return iam_client().create_policy(PolicyName=policyname,
                                      PolicyDocument=json.dumps(policy_doc))


def attach_user_policy(username, policyarn):
    return iam_client().attach_user_policy(UserName=username,
                                           PolicyArn=policyarn)


def attach_group_policy(groupname, policyarn):
    return iam_client().attach_group_policy(GroupName=groupname,
                                            PolicyArn=policyarn)


def add_user_to_group(groupname, username):
    return iam_client().add_user_to_group(UserName=username, GroupName=groupname)

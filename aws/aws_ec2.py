import datetime
import json
import logging
import re
import sys
import time
from typing import Optional

import boto3
import botocore.exceptions
from pytz import utc

from aws import iam_client
from aws.aws_backend import AWSBackend
from utils.Meta import Singleton
from utils.constant import Const
from boto3_type_annotations.ec2 import Client, ServiceResource, Instance
from botocore.exceptions import ClientError


class VPCManager:
    def __init__(self):
        self._ec2cli: Client = AWSBackend().get_client(service='ec2')
        self.logger = logging.getLogger(VPCManager.__class__.__name__)

    def _name_it(self, resource_id, name):
        return self._ec2cli.create_tags(
            Resources=[resource_id],
            Tags=[{
                "Key": "Name",
                "Value": name
            }]
        )

    def create_vpc(self, name, cidr_block="10.0.0.0/16"):
        response = self._ec2cli.create_vpc(
            CidrBlock=cidr_block
        )
        vpc_id = response["Vpc"]["VpcId"]
        self._name_it(vpc_id, name)
        self.logger.info("Created vpc %s with cidr block  %s ", name, cidr_block)
        return vpc_id

    def create_igw(self):
        response = self._ec2cli.create_internet_gateway()
        self.logger.info("created an internet gateway")
        return response["InternetGateway"]["InternetGatewayId"]

    def attach_igw2vpc(self, igw_id, vpc_id):
        self.logger.info("Attaching IGW %s to VPC %s", igw_id, vpc_id)
        return self._ec2cli.attach_internet_gateway(
            InternetGatewayId=igw_id,
            VpcId=vpc_id
        )

    def create_subnet(self, name, vpc_id, cidr_block):
        response = self._ec2cli.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr_block
        )
        subnet_id = response["Subnet"]["SubnetId"]
        self._name_it(subnet_id, name)
        self.logger.info("Created a subnet for VPC %s with CIDR block %s", vpc_id, cidr_block)
        return subnet_id

    def create_routing_table(self, vpc_id):
        response = self._ec2cli.create_route_table(VpcId=vpc_id)
        self.logger.info("Created a routing table for VPC  %s", vpc_id)
        return response["RouteTable"]["RouteTableId"]

    def add_igw_route(self, rtb_id, igw_id, dest_cidr="0.0.0.0/0"):
        self.logger.info("adding route for igw %s to the route table %s", igw_id, rtb_id)
        return self._ec2cli.create_route(
            RouteTableId=rtb_id,
            GatewayId=igw_id,
            DestinationCidrBlock=dest_cidr
        )

    def route_subnet(self, subnet_id, rtb_id):
        self.logger.info("routing subnet %s with routing table %s", subnet_id, rtb_id)
        return self._ec2cli.associate_route_table(
            SubnetId=subnet_id,
            RouteTableId=rtb_id
        )

    def enable_auto_ip(self, subnet_id):
        self.logger.info("enabling the subnet %s for auto ip assignment", subnet_id)
        return self._ec2cli.modify_subnet_attribute(
            SubnetId=subnet_id,
            MapPublicIpOnLaunch={"Value": True}
        )


class IPPermissions:
    @staticmethod
    def ssh_access():
        return {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
        };

    @staticmethod
    def http_access():
        return {
            "IpProtocol": "tcp",
            "FromPort": 80,
            "ToPort": 80,
            "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
        };


class InstanceType:
    def __init__(self, name, cpu, ram_gb):
        self._name = name
        self._cpu = cpu
        self.ram_gb = ram_gb
        self.prices = []
        self.avg_price = 0.0
        self.max_price = 0.0
        self.calc_cost = sys.maxsize
        self.metric_cost = sys.maxsize

    def calc_avg(self):
        if sum(self.prices) > 0:
            self.avg_price = (sum(self.prices) / len(self.prices))
        else:
            self.avg_price = sys.maxsize

    def add_price(self, price):
        self.prices.append(price)
        if price > self.max_price:
            self.max_price = price

    def calculate_metric_cost(self, metric):
        if self.avg_price <= 0:
            return
        if metric == "ram":
            self.metric_cost = self.avg_price / self.ram_gb
        else:
            self.metric_cost = self.avg_price / self._cpu

    def calculate_ratio(self, ratio):
        if self.avg_price <= 0:
            return
        else:
            cpu, ram = ratio.split(':')
            num_of_cont = min((self._cpu / int(cpu)), (self.ram_gb / int(ram)))
            if num_of_cont > 0:
                self.metric_cost = self.avg_price / num_of_cont

    def to_json(self):
        return {"name": self._name, "cpu": self._cpu, "ram": self.ram_gb,
                "avg_price": self.avg_price, "max_price": self.max_price}

    @property
    def name(self):
        return self._name

    @property
    def ram(self):
        return self.ram_gb

    @property
    def cpu(self):
        return self._cpu


class T2Instances:
    @staticmethod
    def nano():
        return InstanceType("t2.nano", 1, 0.5)

    @staticmethod
    def micro():
        return InstanceType("t2.micro", 1, 1)

    @staticmethod
    def small():
        return InstanceType("t2.small", 1, 2)

    @staticmethod
    def medium():
        return InstanceType("t2.medium", 2, 4)

    @staticmethod
    def large():
        return InstanceType("t2.large", 2, 8)

    @staticmethod
    def xlarge():
        return InstanceType("t2.xlarge", 4, 16)

    @staticmethod
    def x2large():
        return InstanceType("t2.2xlarge", 8, 32)


# Note to myself.
# Roles are -> "what can i do?" - "What am i permitted"
# Profiles -> "Who am i ? " +  " What am i permitted"
class EC2AccessManager(Const):
    SSM_ROLE = "SessionManagerInstanceProfile"
    EC2_TRUST_POLICY = '''{
        		    "Version": "2012-10-17",
        		    "Statement": [{
        				    "Effect": "Allow",
        				    "Principal": {
        				        "Service": "ec2.amazonaws.com"
        				    },
        				    "Action": "sts:AssumeRole"
        		            }]
        		    }'''

    EC2_DEFAULT_PROFILE = 'EC2_DEFAULT_PROFILE'

    def __init__(self):
        self._ec2res: ServiceResource = AWSBackend().get_resource(service='ec2')
        self._ec2cli: Client = AWSBackend().get_client(service='ec2')
        self._iam_client = iam_client()
        self._vpc_handler = VPCManager()

    def create_pair(self, key_name):
        return self._ec2cli.create_key_pair(KeyName=key_name)

    def get_key_pairs(self):
        key_pair_list = []
        try:
            describe_key_pairs_response = self._ec2cli.describe_key_pairs()
        except ClientError as error:
            raise Exception("Error utilizing AWS credentials", error)

        for key_pair in describe_key_pairs_response.get("KeyPairs"):
            key_pair_list.append(key_pair.get("KeyName"))
        return key_pair_list

    def get_default_vpc_id(self):
        vpcs_response = self._ec2cli.describe_vpcs(
            Filters=[
                {"Name": "isDefault", "Values": ["true"]},
            ],
        )
        return vpcs_response["Vpcs"][0]["VpcId"]

    def get_default_subnet_id(self):
        describe_subnets_response = self._ec2cli.describe_subnets()
        return describe_subnets_response["Subnets"][-1]["SubnetId"]

    def get_security_group_id(self, sg_name):
        response = self._ec2cli.describe_security_groups(
            GroupNames=[
                sg_name,
            ]
        )
        return response["SecurityGroups"][0]["GroupId"]

    def new_no_internet_vpc(self, name, subnet_cidr="10.0.2.0/24"):
        vpc_id = self._vpc_handler.create_vpc(name + "_VPC")
        subnet_id = self._vpc_handler.create_subnet(name + "_subnet", vpc_id, subnet_cidr)
        rt_id = self._vpc_handler.create_routing_table(vpc_id)
        self._vpc_handler.route_subnet(subnet_id, rt_id)
        self._vpc_handler.enable_auto_ip(subnet_id)
        return vpc_id, rt_id, subnet_id

    def new_internet_vpc(self, name, subnet_cidr):
        vpc_id, rt_id, subnet_id = self.no_internet_vpc(name, subnet_cidr)
        gw_id = self._vpc_handler.create_igw()
        self._vpc_handler.attach_igw2vpc(gw_id, vpc_id)
        self._vpc_handler.add_igw_route(rt_id, gw_id)
        return vpc_id, rt_id, subnet_id

    def create_security_group(self, group_name, desc, vpc_id):
        response = self._ec2cli.create_security_group(
            GroupName=group_name,
            Description=desc,
            VpcId=vpc_id
        )
        return response["GroupId"]

    def add_access_rule(self, sg_id, ingress):
        try:
            self._ec2cli.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[ingress])
        except botocore.exceptions.ClientError as e:
            print('Ingress "' + json.dumps(ingress) + '" already exists. Continuing ...')


    def auto_ip(self, subnet_id):
        self._vpc_handler.enable_auto_ip(subnet_id)

    def _create_ssm_role(self):
        try:
            response = self._iam_client.create_role(
                RoleName=EC2AccessManager.SSM_ROLE,
                AssumeRolePolicyDocument=EC2AccessManager.EC2_TRUST_POLICY,
                Description="IAM Role for connecting to EC2 instance with Session Manager",
            )
            role_name = response["Role"]["RoleName"]
            self._iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonSSMFullAccess",
            )
            return role_name

        except self._iam_client.exceptions.EntityAlreadyExistsException:
            print('Role with name "' + EC2AccessManager.EC2_DEFAULT_PROFILE + '" already exists. Continuing ...')
            return EC2AccessManager.SSM_ROLE

    def _create_default_profile(self, role_name):
        try:
            response = self._iam_client.create_instance_profile(
                InstanceProfileName=EC2AccessManager.EC2_DEFAULT_PROFILE,
                Path='/')

            self._iam_client.add_role_to_instance_profile(
                InstanceProfileName=response['InstanceProfile']['InstanceProfileName'],
                RoleName=role_name)
            # wait for instance profile to be registered
            time.sleep(10)
        except self._iam_client.exceptions.EntityAlreadyExistsException:
            print(
                'Instance profile with name "' + EC2AccessManager.EC2_DEFAULT_PROFILE + '" already exists. Continuing ...')
        return EC2AccessManager.EC2_DEFAULT_PROFILE

    def _get_default_instance_profile(self):
        def find_profile_by_name():
            response = self._iam_client.list_instance_profiles()
            if response is not None:
                for temp_instance_profile in response['InstanceProfiles']:
                    if temp_instance_profile['InstanceProfileName'] == EC2AccessManager.EC2_DEFAULT_PROFILE:
                        return temp_instance_profile
            return None

        instance_profile = find_profile_by_name()

        if instance_profile is None:
            self._create_default_profile(self._create_ssm_role())
            instance_profile = find_profile_by_name()
            if instance_profile is None:
                print('Unable to create instance policy!')
                return None
            else:
                print('Successfully created instance profile "' + EC2AccessManager.EC2_DEFAULT_PROFILE + '"')
                return instance_profile
        else:
            return instance_profile

    def remove_profile(self):
        pass
        #self._iam_client.delete_instance_profile(InstanceProfileName=EC2AccessManager.EC2_DEFAULT_PROFILE)

    def attach_profile(self, inst_id, inst_profile=None):
        if inst_profile is None:
            inst_profile = self._get_default_instance_profile()

        if inst_profile is None:
            print('Unable to get default instance profile.')
            return False

        self._ec2cli.associate_iam_instance_profile(
            IamInstanceProfile={
                'Arn': inst_profile['Arn'],
                'Name': inst_profile['InstanceProfileName']},
            InstanceId=inst_id)

        return True


class InstanceState(Const):
    PENDING = [0, 'pending']
    RUNNING = [16, 'running']
    SHUTTING = [32, 'shutting-down']
    TERMINATED = [48, 'terminated']
    STOPPING = [64, 'stopping']
    STOPPED = [80, 'stopped']

    @staticmethod
    def code(state):
        return state[0]

    @staticmethod
    def status(state):
        return state[1]


@Singleton
class EC2InstHelper:
    def __init__(self):
        self._ec2res: ServiceResource = AWSBackend().get_resource(service='ec2')
        self._ec2cli: Client = AWSBackend().get_client(service='ec2')

    def _get_instance_state(self, inst_id):
        for instance in self._ec2res.instances.all():
            if instance.id == inst_id:
                return instance.state['Name']
        return InstanceState.status(InstanceState.PENDING)

    def get_instance_statuses(self, inst_ids, max_retry=1):
        response = self._ec2cli.describe_instance_status(
            InstanceIds=inst_ids
        )
        while not response['InstanceStatuses'] and max_retry > 0:
            response = self._ec2cli.describe_instance_status(
                InstanceIds=inst_ids
            )
            max_retry -= 1
        return response['InstanceStatuses']

    # running vs stopped
    def get_instance_status(self, inst_id, max_retry=5, default=None):
        instances = self.get_instance_statuses([inst_id], max_retry=max_retry)
        if not instances:
            if default:
                return default
            else:
                return self._get_instance_state(inst_id)
        else:
            return instances[0]['InstanceState']['Name']

    def get_instance_state(self, instance_id, max_retry=5):
        instances = self.get_instance_statuses([instance_id],max_retry=max_retry)
        if instances:
            for instance in instances:
                if instance['InstanceId'] == instance_id:
                    return instance['InstanceStatus']['Details'][0]['Status']
        else:
            return 'Initializing'

    @staticmethod
    def get_tag_value(instance, key):
        if instance.tags is None:
            return None
        for tag in instance.tags:
            if tag['Key'] == key:
                return tag['Value']
        return None

    @staticmethod
    def filter_instances_by_function(f, instances):
        return list(filter(f, instances))

    @staticmethod
    def filter_instances_by_tag(tag: tuple, instances):
        assert len(tag) == 2, "tag must be key/value pair"
        return EC2InstHelper.filter_instances_by_function(
            lambda i: False if i.tags is None else any(
                map(lambda t: t['Key'] == tag[0] and t['Value'] == tag[1], i.tags)), instances)

    @staticmethod
    def refresh_instances(instances):
        EC2InstHelper().get_instance_statuses(list(map(lambda i: i.id, instances)))

    @staticmethod
    def _status_check(instance, status):
        return EC2InstHelper().get_instance_status(instance.id) == status

    @staticmethod
    def is_ready(instance):
        return EC2InstHelper().get_instance_state(instance.id) == 'passed'

    @staticmethod
    def is_terminated(instance):
        return EC2InstHelper._status_check(instance, InstanceState.status(InstanceState.TERMINATED))

    @staticmethod
    def is_stopped(instance):
        return EC2InstHelper._status_check(instance, InstanceState.status(InstanceState.STOPPED))

    @staticmethod
    def is_running(instance):
        return EC2InstHelper._status_check(instance, InstanceState.status(InstanceState.RUNNING))

    @staticmethod
    def get_running(instances):
        return EC2InstHelper.filter_instances_by_function(EC2InstHelper.is_running, instances)

    @staticmethod
    def get_stopped(instances):
        return EC2InstHelper.filter_instances_by_function(EC2InstHelper.is_stopped, instances)

    @staticmethod
    def get_terminated(instances):
        return EC2InstHelper.filter_instances_by_function(EC2InstHelper.is_terminated, instances)

    @staticmethod
    def start_instances(instances):
        for inst in instances:
            inst.start()

    @staticmethod
    def stop_instances(instances):
        for inst in instances:
            inst.stop()

    @staticmethod
    def terminate_instances(instances):
        for inst in instances:
            if not EC2InstHelper.is_terminated(inst):
                inst.terminate()

    @staticmethod
    def wait_for_state(state_cb, instances, max_retry=5, interval=12, success_msg=None):
        if success_msg is None:
            success_msg = "Instances are ready as you like"
        retries = max_retry
        while retries > 0:
            if state_cb(instances):
                print(success_msg)
                return True
            retries -= 1
            print("Waiting for desired state... (retries remaining: {})".format(retries))
            time.sleep(interval)

        print("Retries exceeded. Proceeding anyway.")
        return False

    @staticmethod
    def check_expired(instance, tag_datetime, max_time):
        if EC2InstHelper.is_terminated(instance):
            return False
        if EC2InstHelper.is_stopped(instance):
            return True
        # launch time
        lt_datetime = instance.launch_time
        # localize
        tag_datetime = utc.localize(tag_datetime)
        # most recent
        recent_time = max(lt_datetime, tag_datetime)
        # delta_time
        delta = utc.localize(datetime.datetime.now()) - recent_time
        uptime = delta.total_seconds()
        return uptime > max_time


class EC2Launcher:
    def __init__(self, type_tag):
        self._ec2res: ServiceResource = AWSBackend().get_resource(service='ec2')
        self._ec2cli: Client = AWSBackend().get_client(service='ec2')
        self._type_tag = type_tag
        self._logger = logging.getLogger(EC2Launcher.__class__.__name__)

    def launch_instance(self,key_name, sg_id, subnet_id, img_id, instance_type: InstanceType, num_inst, userdata=''):
        instances = self._ec2res.create_instances(
            ImageId=img_id,
            MinCount=num_inst,
            MaxCount=num_inst,
            InstanceType=instance_type.name,
            SecurityGroupIds=[sg_id],
            SubnetId=subnet_id,
            KeyName=key_name,
            UserData=userdata
        )

        for inst in instances:
            self.tag_instance(inst.id, self._type_tag)

        return instances

    def get_ami(self, template_name):
        images = self._ec2cli.describe_images(Owners=['self'])['Images']
        chosen_image = (None, -1)
        for image in images:
            if re.match(template_name, image['Name']):
                tmp_ver = int((re.search(r'\d+', image['Name'])).group(0))
                if tmp_ver > chosen_image[1]:
                    selected_image = (image, tmp_ver)

            if chosen_image[1] == -1:
                raise KeyError('AMI with name "' + template_name + '" not found')
            else:
                return chosen_image

    def tag_instance(self, inst_id, *tags: tuple):
        assert all(map(lambda t: len(t) == 2, tags)), "all pairs should key/value pairs"
        self._ec2res.create_tags(Resources=[inst_id],
                                 Tags=[{'Key': tup[0], 'Value': tup[1]} for tup in tags])

    def get_instances(self, inst_ids):
        return list(self._ec2res.instances.filter(InstanceIds=inst_ids))

    def get_instance(self, inst_id) -> Optional[Instance]:
        insts = self.get_instances([inst_id])
        if insts:
            return insts[0]
        else:
            return None

    def enable_api_termination(self, inst_id):
        return self._ec2cli.modify_instance_attribute(
            InstanceId=inst_id,
            DisableApiTermination={"Value": False}
        )

    def all_instances(self):
        all_instances = list(self._ec2res.instances.all())
        return EC2InstHelper.filter_instances_by_tag(self._type_tag, all_instances)

    def terminate_instances(self):
        return EC2InstHelper.terminate_instances(self.all_instances())

    def refresh_instances(self):
        return EC2InstHelper.refresh_instances(self.all_instances())

    def get_running_instances(self):
        return EC2InstHelper.get_running(self.all_instances())

    def get_stopped_instances(self):
        return EC2InstHelper.get_stopped(self.all_instances())

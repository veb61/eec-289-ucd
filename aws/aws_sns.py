import json
import logging
import boto3
from boto3_type_annotations.sns import ServiceResource, Topic, Subscription
from botocore.exceptions import ClientError


def sns_resource():
    sns: ServiceResource = boto3.resource('sns')
    return sns


def sns_logger():
    return logging.getLogger("SNS_LOGGER")


def create_topic(name):
    try:
        topic: Topic = sns_resource().create_topic(Name=name)
        sns_logger().info("Created topic %s with ARN %s.", name, topic.arn)
    except ClientError:
        sns_logger().exception("Couldn't create topic %s.", name)
        raise
    else:
        return topic


def list_topics():
    try:
        topics_iter = sns_resource().topics.all()
        sns_logger().info("Got topics.")
    except ClientError:
        sns_logger().exception("Couldn't get topics.")
        raise
    else:
        return topics_iter


def create_or_get_topic(name):
    for topic in list_topics():
        if topic.attributes['DisplayName'] == name:
            return topic
    return create_topic(name)


def delete_topic(topic):
    try:
        topic.delete()
        sns_logger().info("Deleted topic %s.", topic.arn)
    except ClientError:
        sns_logger().exception("Couldn't delete topic %s.", topic.arn)
        raise


def subscribe(topic, protocol, endpoint):
    try:
        subscription: Subscription = topic.subscribe(
            Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True)
        sns_logger().info("Subscribed %s %s to topic %s.", protocol, endpoint, topic.arn)
    except ClientError:
        sns_logger().exception(
            "Couldn't subscribe %s %s to topic %s.", protocol, endpoint, topic.arn)
        raise
    else:
        return subscription


def list_subscriptions(self, topic=None):
    try:
        if topic is None:
            subs_iter = sns_resource().subscriptions.all()
        else:
            subs_iter = topic.subscriptions.all()
        sns_logger().info("Got subscriptions.")
    except ClientError:
        sns_logger().exception("Couldn't get subscriptions.")
        raise
    else:
        return subs_iter


def add_subscription_filter(subscription: Subscription, attributes):
    try:
        att_policy = {key: [value] for key, value in attributes.items()}
        subscription.set_attributes(
            AttributeName='FilterPolicy', AttributeValue=json.dumps(att_policy))
        sns_logger().info("Added filter to subscription %s.", subscription.arn)
    except ClientError:
        sns_logger().exception(
            "Couldn't add filter to subscription %s.", subscription.arn)
        raise


def delete_subscription(subscription: Subscription):
    try:
        subscription.delete()
        sns_logger().info("Deleted subscription %s.", subscription.arn)
    except ClientError:
        sns_logger().exception("Couldn't delete subscription %s.", subscription.arn)
        raise


@staticmethod
def add_subscription_filter(subscription, attributes):
    try:
        att_policy = {key: [value] for key, value in attributes.items()}
        subscription.set_attributes(
            AttributeName='FilterPolicy', AttributeValue=json.dumps(att_policy))
        sns_logger().info("Added filter to subscription %s.", subscription.arn)
    except ClientError:
        sns_logger().exception(
            "Couldn't add filter to subscription %s.", subscription.arn)
        raise


def publish_message(topic: Topic, message, attributes={}):
    try:
        att_dict = {}
        for key, value in attributes.items():
            if isinstance(value, str):
                att_dict[key] = {'DataType': 'String', 'StringValue': value}
            elif isinstance(value, bytes):
                att_dict[key] = {'DataType': 'Binary', 'BinaryValue': value}
        response = topic.publish(Message=message, MessageAttributes=att_dict)
        message_id = response['MessageId']
        sns_logger().info(
            "Published message with attributes %s to topic %s.", attributes,
            topic.arn)
    except ClientError:
        sns_logger().exception("Couldn't publish message to topic %s.", topic.arn)
        raise
    else:
        return message_id

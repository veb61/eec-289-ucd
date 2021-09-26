import logging
import boto3

from boto3_type_annotations.sqs import ServiceResource, Client, Queue
from botocore.exceptions import ClientError


class SqsHandler:
    def __init__(self, location):
        self.sqs: ServiceResource = boto3.resource('sqs', region_name=location)
        self.logger = logging.getLogger(SqsHandler.__class__.__name__)

    def create_queue(self, name, attributes={}):
        try:
            queue: Queue = self.sqs.create_queue(
                QueueName=name,
                Attributes=attributes
            )
            self.logger.info("Created queue '%s' with URL=%s", name, queue.url)
        except ClientError as error:
            self.logger.exception("Couldn't create queue named '%s'.", name)
            raise error
        else:
            return queue

    def get_queue(self, name, raiseit: bool = True):
        try:
            queue: Queue = self.sqs.get_queue_by_name(QueueName=name)
            self.logger.info("Got queue '%s' with URL=%s", name, queue.url)
        except ClientError as error:
            if raiseit:
                self.logger.warning("Couldn't get queue named %s.", name)
                raise error
        else:
            return queue

    def get_queue_by_url(self, url):
        return self.sqs.Queue(url=url)

    def get_or_create_queue(self, name):
        queue: Queue = self.get_queue(name, False)
        if not queue:
            queue = self.create_queue(name)
        return queue

    def queue_exists(self, name):
        try:
            queue: Queue = self.sqs.get_queue_by_name(QueueName=name)
            exists = True
        except ClientError:
            exists = False
        return exists

    def get_queues(self, prefix=None):
        if prefix:
            queue_iter = self.sqs.queues.filter(QueueNamePrefix=prefix)
        else:
            queue_iter = self.sqs.queues.all()
        queues = list(queue_iter)
        if queues:
            self.logger.info("Got queues: %s", ', '.join([q.url for q in queues]))
        else:
            self.logger.warning("No queues found.")
        return queues

    def remove_queue(self, queue: Queue):
        try:
            queue.delete()
            self.logger.info("Deleted queue with URL=%s.", queue.url)
        except ClientError as error:
            self.logger.exception("Couldn't delete queue with URL=%s!", queue.url)
            raise error

    def send_message(self, queue, message_body, message_attributes={}):
        try:
            response = queue.send_message(
                MessageBody=message_body,
                MessageAttributes=message_attributes
            )
        except ClientError as error:
            self.logger.exception("Send message failed: %s", message_body)
            raise error
        else:
            return response

    def send_messages(self, queue, messages):
        try:
            entries = [{
                'Id': str(ind),
                'MessageBody': msg['body'],
                'MessageAttributes': msg['attributes']
            } for ind, msg in enumerate(messages)]
            response = queue.send_messages(Entries=entries)
            if 'Successful' in response:
                for msg_meta in response['Successful']:
                    self.logger.info(
                        "Message sent: %s: %s",
                        msg_meta['MessageId'],
                        messages[int(msg_meta['Id'])]['body']
                    )
            if 'Failed' in response:
                for msg_meta in response['Failed']:
                    self.logger.warning(
                        "Failed to send: %s: %s",
                        msg_meta['MessageId'],
                        messages[int(msg_meta['Id'])]['body']
                    )
        except ClientError as error:
            self.logger.exception("Send messages failed to queue: %s", queue)
            raise error
        else:
            return response

    def receive_messages(self, queue, max_number, wait_time=None):
        try:
            messages = queue.receive_messages(
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_number,
                WaitTimeSeconds=wait_time
            )
            for msg in messages:
                self.logger.info("Received message: %s: %s", msg.message_id, msg.body)
        except ClientError as error:
            self.logger.exception("Couldn't receive messages from queue: %s", queue)
            raise error
        else:
            return messages

    def delete_message(self, message):
        try:
            message.delete()
            self.logger.info("Deleted message: %s", message.message_id)
        except ClientError as error:
            self.logger.exception("Couldn't delete message: %s", message.message_id)
            raise error

    def delete_messages(self, queue, messages):
        try:
            entries = [{
                'Id': str(ind),
                'ReceiptHandle': msg.receipt_handle
            } for ind, msg in enumerate(messages)]
            response = queue.delete_messages(Entries=entries)
            if 'Successful' in response:
                for msg_meta in response['Successful']:
                    self.logger.info("Deleted %s", messages[int(msg_meta['Id'])].receipt_handle)
            if 'Failed' in response:
                for msg_meta in response['Failed']:
                    self.logger.warning(
                        "Could not delete %s",
                        messages[int(msg_meta['Id'])].receipt_handle
                    )
        except ClientError:
            self.logger.exception("Couldn't delete messages from queue %s", queue)
        else:
            return response

    @staticmethod
    def get_message_cnt(queue: Queue):
        return int(queue.attributes['ApproximateNumberOfMessages'])

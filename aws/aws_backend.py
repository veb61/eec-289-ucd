import logging
import sys

import boto3
from botocore.config import Config
from utils.Meta import Singleton


@Singleton
class AWSBackend:
    def __init__(self):
        self._logger = logging.getLogger(AWSBackend.__class__.__name__)

    def get_available_regions(self, service: str):
        """AWS exposes their list of regions as an API. Gather the list."""
        regions = boto3.session.Session().get_available_regions(service)
        self._logger.debug(
            "The service %s does not have available regions. Returning us-west-1 as default"
        )
        if not regions:
            regions = ["us-west-1"]
        return regions

    def get_client(self, service: str, profile: str = None, region: str = 'us-west-1') -> boto3.Session.client:
        """Get a boto3 client for a given service"""
        logging.getLogger("botocore").setLevel(logging.CRITICAL)
        session_data = {"region_name": region}
        if profile:
            session_data["profile_name"] = profile
        session = boto3.Session(**session_data)
        if region not in self.get_available_regions(service):
            self._logger.debug(f"The service {service} is not available in this region!")
            sys.exit()
        config = Config(read_timeout=5, connect_timeout=5, retries={"max_attempts": 10})
        client = session.client(service, config=config)
        self._logger.debug(
            f"{client.meta.endpoint_url} in {client.meta.region_name}: boto3 client login successful"
        )
        return client

    def get_resource(self,
            service: str, profile: str = None, region: str = "us-west-1"
    ) -> boto3.Session.resource:
        """Get a boto3 resource for a given service"""
        logging.getLogger("botocore").setLevel(logging.CRITICAL)
        session_data = {"region_name": region}
        if profile:
            session_data["profile_name"] = profile
        session = boto3.Session(**session_data)

        resource = session.resource(service)
        return resource



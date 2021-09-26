#!/usr/bin/env python3

import argparse
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from common.configuration import AWSConfig
from common.protocol import AWSIDRegistration
from student.tasks import AWSIssuer

if __name__ == '__main__':
    aws_parser = argparse.ArgumentParser(description='AWSID Operations ',
                                         epilog='Enjoy the program! :)')

    # aws config
    awscfg = aws_parser.add_mutually_exclusive_group(required=True)

    awscfg.add_argument('--configurl',
                        action='store_const',
                        const="https://raw.githubusercontent.com/eec-ucd/eec289/main/config.aws",
                        help='configuration url for the aws server')

    awscfg.add_argument('--configfile',
                        action='store_const',
                        const='config.aws',
                        help='configuration file for the aws server')

    aws_parser.add_argument('--id', required=True, type=str, help="AWS ID")
    aws_parser.add_argument('--email', required=True, type=str, help="UCD Email")

    args = aws_parser.parse_args()

    if args.configurl:
        awsconfig = AWSConfig.load_url(args.configurl)
    elif args.configfile:
        awsconfig = AWSConfig.load_file(args.configfile)

    issuer = AWSIssuer(awsconfig)
    issuer.issue(AWSIDRegistration(args.id, args.email))

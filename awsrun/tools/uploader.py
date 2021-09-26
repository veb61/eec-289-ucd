#!/usr/bin/env python3

import argparse
from os import path
import sys

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from common.commands import Upload
from common.configuration import AWSConfig
from common.resources import S3Path

if __name__ == '__main__':
    aws_parser = argparse.ArgumentParser(description='Uploads your files to S3Bucket',
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

    aws_parser.add_argument('--path',
                            type=str,
                            required=True,
                            help="file you want to download")

    aws_parser.add_argument('--key',
                            type=str,
                            required=True,
                            help="key of the file you want to download")

    args = aws_parser.parse_args()

    if args.configurl:
        awsconfig = AWSConfig.load_url(args.configurl)
    elif args.configfile:
        awsconfig = AWSConfig.load_file(args.configfile)

    Upload(awsconfig.serverpath, awsconfig.bucketpath, S3Path(args.path, args.key)).execute()

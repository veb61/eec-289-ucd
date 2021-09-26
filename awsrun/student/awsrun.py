#!/usr/bin/env python3
import argparse
from os import path
import sys

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from common.configuration import CmdConfig, WSConfig, AWSConfig
from common.protocol import IOTask
from student.tasks import AWSIssuer


class CoreRange:
    def __init__(self, imin=1, imax=1):
        self.imin = imin
        self.imax = imax

    def __call__(self, arg):
        try:
            value = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Must be an integer")

        if self.imin < 0 or self.imax < 0:
            raise argparse.ArgumentTypeError(f"Core numbers can only be positive")

        if value < self.imin:
            raise argparse.ArgumentTypeError(f"Must be an integer >= {self.imin}")

        if value > self.imax:
            raise argparse.ArgumentTypeError(f"Must be an integer <= {self.imax}")

        return value


if __name__ == '__main__':
    aws_parser = argparse.ArgumentParser(description='Runs your program on AWS',
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

    # task config
    aws_parser.add_argument('--cmd',
                            type=str,
                            required=True,
                            help='command to run (executable with arguments)')

    aws_parser.add_argument('--deps',
                            type=str,
                            default="deps.aws",
                            help='config file holding the relative paths')

    aws_parser.add_argument('--workfolder',
                            type=str,
                            default="/tmp/std-submissions",
                            help='work folder')

    aws_parser.add_argument('--timeout',
                            type=int,
                            default=60,
                            help='task timeout')

    aws_parser.add_argument('--perf',
                            type=str,
                            default="",
                            help='performance')

    aws_parser.add_argument('--core',
                            type=CoreRange(1, 8),
                            default=1,
                            help='is this a multicore run')

    # workspace config
    aws_parser.add_argument('--prefix',
                            type=str,
                            default="submission",
                            help='prefix for job folders')

    args = aws_parser.parse_args()

    print(args)

    if args.configurl:
        awsconfig = AWSConfig.load_url(args.configurl)
    elif args.configfile:
        awsconfig = AWSConfig.load_file(args.configfile)

    cmdconfig = CmdConfig(cmd=args.cmd.split(),
                          timeout=args.timeout,
                          cores=args.core,
                          depfile=args.deps)

    wsconfig = WSConfig(args.prefix)

    issuer = AWSIssuer(awsconfig)

    task = IOTask(cmdconfig, wsconfig, args.workfolder, args.perf)

    issuer.issue(task)

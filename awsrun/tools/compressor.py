#!/usr/bin/env python3
import argparse
from os import path
import sys

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from common.commands import Compress
from common.resources import OSPath, File

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tars your files',
                                     epilog='Enjoy the program! :)')

    parser.add_argument('--paths',
                        nargs='+',
                        required=True,
                        help="paths seperated by space to tar")

    parser.add_argument('--target',
                        type=str,
                        required=True,
                        help="target tar file to create")

    args = parser.parse_args()

    ospaths = tuple(map(lambda p: OSPath.new(p), args.paths))
    target = File(args.target)

    Compress(target, *ospaths).execute()

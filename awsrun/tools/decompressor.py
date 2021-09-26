#!/usr/bin/env python3
import argparse
from os import path
import sys

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from common.commands import Decompress
from common.resources import Folder, File

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Untars your files',
                                     epilog='Enjoy the program! :)')

    parser.add_argument('--tarfile',
                        type=str,
                        required=True,
                        help="tar file to untar")

    parser.add_argument('--target',
                        type=str,
                        required=True,
                        help="target folder")

    parser.add_argument('--files',
                        nargs='+',
                        default=[],
                        help="files to extract, if you don't specify all will be extracted")

    args = parser.parse_args()

    filters = tuple(map(lambda f: File(f), args.files))

    Decompress(Folder(args.target), File(args.tarfile), *filters).execute()

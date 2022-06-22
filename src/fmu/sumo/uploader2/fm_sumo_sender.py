#!/usr/bin/env python

import zmq
from argparse import ArgumentParser
import os
import glob


def main():
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    print(f"Reciever host: " + args.host)
    print(f"Reciever port: " + args.port)
    print(f"Searchpath: " + args.searchpath)

    send(args.host, args.port, args.searchpath)


def send(host, port, searchpath):
    files = [full_path(f) for f in glob.glob(searchpath) if os.path.isfile(f)]
    num_of_files = len(files)

    print(f"Found {num_of_files} files")

    context = zmq.Context()
    s = context.socket(zmq.PUSH)
    s.connect(f"tcp://{host}:{port}")

    for index, file in enumerate(files):
        file_num = index + 1
        print(f"Sending {file_num}/{num_of_files}: {file}")
        s.send_string(file)

    s.close()
    context.term()

    print("Done!")


def full_path(path):    
    return os.path.abspath(path)


def get_arg_parser():
    arg_parser = ArgumentParser()

    arg_parser.add_argument(
        "host", 
        type=str,
        help="Host of sumo reciever"
    )

    arg_parser.add_argument(
        "port", 
        type=str,
        help="Port of sumo reciever"
    )

    arg_parser.add_argument(
        "searchpath", 
        type=str, 
        help="Absolute search path for files to upload"
    )

    return arg_parser


if __name__ == "__main__":
    main()
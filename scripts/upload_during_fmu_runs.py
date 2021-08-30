import os
import argparse
from fmu.sumo import uploader

"""
        Script for uploading to Sumo intended to be run as part of FMU worflow.

"""


def main():
    args = parse_arguments()

    print('\n\n =======')
    print(f'searchpath: {args.searchpath}')
    print('\n\n =======\n')

    # establish the connection
    sumo_connection = uploader.SumoConnection(env=args.env)

    # initiate the case on disk object
    case = uploader.CaseOnDisk(case_metadata_path=args.case_metadata_path, sumo_connection=sumo_connection)

    # add files to the case on disk object
    case.add_files(os.path.join(args.searchpath))

    # upload the indexed files
    case.upload(threads=args.threads, register_case=False)  # registration should have been done by HOOK workflow

    # tmp: Let me know that main has finished
    print(f'Searchpath was: {args.searchpath}')
    print('upload_during_fmu_runs.py:main() has ended')


def parse_arguments():
    """

        Parse the arguments

        Returns:
            args: argparse.ArgumentParser() object

    """

    parser = argparse.ArgumentParser()
    parser.add_argument('case_metadata_path', type=str, help='Absolute path to run case_metadata')
    parser.add_argument('searchpath', type=str, help='Globable search path for files to upload')
    parser.add_argument('env', type=str, help="Which environment to use.")
    parser.add_argument('threads', type=int, help="Set number of threads to use.")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp', 'fmu', 'preview']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, fmu')

    return args


if __name__ == '__main__':
    main()

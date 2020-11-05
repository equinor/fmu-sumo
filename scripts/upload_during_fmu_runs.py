import argparse
from fmu import sumo
import os
import logging

"""
        Script for uploading to Sumo intended to be run as part of FMU worflow on Johan Sverdrup
        
        This is the Python script called by the shell script called by the ERT FORWARD_JOB

"""

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger()

#logger.setLevel(logging.DEBUG)

def main():

    args = parse_arguments()

    # establish the connection
    sumo_connection = sumo.SumoConnection(env=args.env)

    # initiate the ensemble on disk object
    e = sumo.EnsembleOnDisk(manifest_path=args.manifest_path, sumo_connection=sumo_connection)

    # add files to the ensemble on disk object
    e.add_files(os.path.join(args.searchpath))

    # upload the indexed files
    #e.upload(threads=args.threads, register_ensemble=False)   # registration should have been done by HOOK workflow

    # tmp: Let me know that main has finished
    logger.debug('upload_during_fmu_runs.py:main() has ended')

def parse_arguments():

    """

        Parse the arguments

        Returns:
            args: argparse.ArgumentParser() object

    """

    parser = argparse.ArgumentParser()
    parser.add_argument('manifest_path', type=str, help='Absolute path to run manifest')
    parser.add_argument('searchpath', type=str, help='Globable search path for files to upload')
    parser.add_argument('env', type=str, help="Which environment to use.")
    parser.add_argument('threads', type=int, help="Set number of threads to use.")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp')

    return args

if __name__ == '__main__':
    main()
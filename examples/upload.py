import argparse
from fmu.sumo import uploader
import os
import logging
import sys
from datetime import datetime
import time

logger = logging.getLogger()
#logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

def main():
    """Script for uploading data to Sumo"""

    args = parse_arguments()
    manifest_path = os.path.join(args.casepath, f'{args.iteration}/share/runinfo/manifest.yaml')

    # add some files
    iteration = 'pred'
    subfolders = [f'{args.iteration}/share/results/maps/depth/*.gri',
                  f'{args.iteration}/share/results/maps/isochores/*.gri',
                  f'{args.iteration}/share/results/maps/depth_conversion/*.gri',
                  f'realization-0/{args.iteration}/share/results/polygons/*--field_outline.csv',
                  f'realization-0/{args.iteration}/share/results/polygons/*--faultlines.csv',
                  f'realization-*/{args.iteration}/share/results/maps/depth/*.gri',
                  f'realization-*/{args.iteration}/share/results/maps/isochores/*.gri',
                  f'realization-*/{args.iteration}/share/results/maps/fwl/*.gri',
                  f'realization-*/{args.iteration}/share/results/maps/depth_conversion/*.gri',
                  #f'realization-0/{args.iteration}/share/results/maps/depth/*.gri',
                  #f'realization-0/{args.iteration}/share/results/maps/isochores/*.gri',
                  ]

    sumo_connection = uploader.SumoConnection(env=args.env)
    e = uploader.EnsembleOnDisk(manifest_path=manifest_path, sumo_connection=sumo_connection)

    e.register()

    for subfolder in subfolders:
        print('Adding files: {}'.format(subfolder))
        e.add_files(os.path.join(args.casepath, subfolder))
    
    _t0 = time.perf_counter()
    print(f'{datetime.isoformat(datetime.now())}: Uploading {len(e.files)} files with {args.threads} threads on environment {args.env}')
    e.upload(threads=args.threads)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('casepath', type=str, help='Absolute path to case root on Scratch')
    parser.add_argument('--env', type=str, default='dev', help="Which environment to use, default: dev")
    parser.add_argument('--threads', type=int, default=4, help="Set number of threads to use. Default: 4.")
    parser.add_argument('--iteration', type=str, default='pred', help="FMU iteration name")
    args = parser.parse_args()

    if args.env not in ['dev', 'test', 'prod', 'exp']:
        raise ValueError(f'Illegal environment: {args.env}. Valid environments: dev, test, prod, exp')

    return args

if __name__ == '__main__':
    main()
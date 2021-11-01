#!/usr/bin/env python3

import argparse
import sys
import os

from tabulate import tabulate

import kbr.config_utils as config_utils
import kbr.args_utils as args_utils
import kbr.version_utils as version_utils
import kbr.log_utils as logger

import k.azure_utils as azure_utils


version = version_utils.as_string('k')
config = None
program_name = 'blob-states'

config = None

def init(args):
    global config
    if args.config and os.path.isfile( args.config ):
        config = config_utils.readin_config_file(args.config)

        logger.init(name=program_name)
        logger.set_log_level(args.verbose)
        logger.info(f'{program_name} (v:{version})')

        azure_utils.connect( config.azure.subscription_id )
        
    else:
        print('config is missing')
        sys.exit()

def main():
    commands = ['add', 'delete', 'list', 'size', 'run-playbook', 'init', 'help']

    parser = argparse.ArgumentParser(description=f'ecc_cli: command line tool for ECC ({version})')

    parser.add_argument('-c', '--config', help="ECC config file",
                        default=args_utils.get_env_var('AZURE_CONF','azure.yml')) 
    parser.add_argument('-v', '--verbose', default=3, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('command', nargs='*', help="{}".format(",".join(commands)))

    args = parser.parse_args()
    init(args)

    azure_utils.storage_containers('neuromics')




if __name__ == "__main__":
    main()

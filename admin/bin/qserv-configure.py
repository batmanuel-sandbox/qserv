#!/usr/bin/env python

import argparse
import ConfigParser
import fileinput
from lsst.qserv.admin import configure, commons, logger
import logging
import os
import shutil
from subprocess import check_output
import sys

def parseArgs():
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    #
    step_list = ['prep','run']
    step_option_values = step_list + ['all']
    parser.add_argument("-s", "--step", dest="step", choices=step_option_values,
        default='run',
        help="Qserv configurator install step : \n'" +
        "     " + step_option_values[0] + " : create a qserv_run_dir " +
        "directory which will contains configuration and execution data " +
        "for a given Qserv instance\n" +
        "     " + step_option_values[1] + " : fill qserv_run_dir " +
        "configuration files with values from meta-config file qserv.conf"
        )

    # run dir, all mutable data related to a qserv running instance are
    # located here
    qserv_version=check_output(["qserv-version.sh"])
    qserv_version = qserv_version.strip(' \t\n\r')
    default_qserv_run_dir=os.path.join(os.path.expanduser("~"),"qserv-run",qserv_version)
    parser.add_argument("-r", "--qserv-run-dir", dest="qserv_run_dir",
            default=default_qserv_run_dir,
            help="Full path to qserv_run_dir"
            )

    # meta-configuration file whose parameters will be dispatched in Qserv
    # services configuration files
    args = parser.parse_args()
    default_meta_config_file = os.path.join(args.qserv_run_dir, "qserv.conf")
    parser.add_argument("-c", "--metaconfig",  dest="meta_config_file",
            default=default_meta_config_file,
            help="Full path to Qserv meta-configuration file"
            )

    args = parser.parse_args()

    if args.step=='all':
        args.step_list = step_list
    else:
        args.step_list = [args.step]

    return args

def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)

def main():

    args = parseArgs()

    logging.basicConfig(level=logging.INFO)

    qserv_dir = os.path.abspath(
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "..")
                )

    if 'prep' in args.step_list:
        template_config_dir = os.path.join( qserv_dir, "admin")

        logging.info("Initializing configuration from {0} to {1}"
            .format(template_config_dir, args.qserv_run_dir))

        copy_and_overwrite(template_config_dir, args.qserv_run_dir)

        for line in fileinput.input(args.meta_config_file, inplace = 1):
            print line.replace("run_base_dir =", "run_base_dir = " + args.qserv_run_dir),

    if 'run' in args.step_list:

        #########################
        #
        # Reading config file
        #
        #########################
        try:
            config = commons.read_config(args.meta_config_file)
        except ConfigParser.NoOptionError, exc:
            logging.fatal("An option is missing in your configuration file: %s" % exc)
            sys.exit(1)

        #####################################
        #
        # Defining main directory structure
        #
        #####################################
        configure.check_root_dirs()
        configure.check_root_symlinks()

        #####################################
        #
        # Templating
        # filling Qserv services config files
        # with qserv-build.conf values
        #
        #####################################
        configure.apply_templates()


        #########################
        #
        # Configure services 
        #
        #########################
        configuration_scripts_dir=os.path.join(config['qserv']['run_base_dir'],'tmp','configure')
        component_list = ['mysql', 'xrootd', 'qserv-czar']

        if config['qserv']['node_type'] in ['mono','worker']:
           component_list.append('scisql')

        for c in component_list:
            script = os.path.join( configuration_scripts_dir, c+".sh")
            commons.run_command(script)

        shell = os.environ.get("EUPS_SHELL", "sh")
        key = 'QSERV_RUN_DIR'
        val = os.path.join(config['qserv']['run_base_dir'])
        if shell in ("sh", "zsh",):
                cmd = ["export", "%s=%s" % (key, val)]
        elif shell in ("csh",):
                cmd = ["setenv", "%s %s" % (key, val)]

        commons.run_command(cmd)

if __name__ == '__main__':
    main()

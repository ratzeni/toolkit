#!/usr/bin/env python3

import os
import sys
import subprocess
import argparse
subprocess.run("pip install oyaml", shell=True)
import oyaml as yaml
import logging


class App(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        self.input_file = args.input_file
        self.folder = args.folder
        self.project = args.project_name if args.project_name else os.getcwd().split('/')[-1]
        self.merge = args.merge_only
        self.config = args.config_only
        self.force = args.force
        self.paired = args.paired
        if os.path.exists(self.folder):
            if not self.force:
                raise ValueError("WARNING: Folder exists. Rerun with the flag '--force'")
            self.logger.info("ALERT: '--force' flag activated: using existing folder")
        else:
            self.logger.info("Creating folder {}".format(self.folder))
            os.mkdir(self.folder)

    def get_read_pair(self, s):
        #self.logger.info("Reading {}".format(self.input_file))
        basename = s.split('/')[-1]
        read = basename.split('_')[3]
        if read in ['R1', 'R2']:
            return read
        else:
            sys.exit()

    def run(self):
        self.logger.info("Reading input file: {}".format(self.input_file))
        with open(self.input_file, "r") as inputfile:
            data = yaml.full_load(inputfile.read())
        reads = {}
        for sample, units in data['samples'].items():
            reads[sample] = {'R1': [],
                             'R2': []}
            for unit in units:
                for f in data['units'][unit]:
                    reads[sample][self.get_read_pair(f)].append(f)

        new_samples = {}
        if self.config: self.logger.info("Skipping merge: --config_only mode activated")
        for s, pairs in reads.items():
            cmd = ['cat']
            for p in pairs['R1']:
                 cmd.append(p)
            cmd.append('>' + os.path.join(self.folder,'{}_R1.fastq.gz'.format(s)))
            if not self.config:
                self.logger.info("Running merge command: {}".format(cmd))
                subprocess.run(' '.join(cmd), shell=True)
            cmd = ['cat']
            for p in pairs['R2']:
                cmd.append(p)
            cmd.append('>' + os.path.join(self.folder,'{}_R2.fastq.gz'.format(s)))
            if not self.config:
                if self.paired:
                    self.logger.info("Paired Reads mode activated: merging R2 reads.")
                    self.logger.info("Running R2 merge command: {}".format(cmd))
                    subprocess.run(' '.join(cmd), shell=True)

            workdir = os.getcwd()
            new_samples[s] = os.path.join(workdir,self.folder,'{}_R1.fastq.gz'.format(s))
        yaml_template = 'config.template.yaml'
        with open(yaml_template, "r") as inputfile:
            new_data = yaml.full_load(inputfile.read())
        new_data['samples'] = new_samples
        yaml_project = 'config.project.{}.yaml'.format(self.project)
        if not self.merge:
            self.logger.info("Writing configfile: {}".format(yaml_project))
            with open(yaml_project, "w") as outfile:
                yaml.dump(new_data, outfile, indent=4)
        if self.merge:
            self.logger.info("Skipping configfile generation: --merge_only mode activated")


def get_logger(name, level="WARNING", filename=None, mode="a"):
    log_format = '%(asctime)s|%(levelname)-8s|%(name)s |%(message)s'
    log_datefmt = '%Y-%m-%d %H:%M:%S'
    logger = logging.getLogger(name)
    if not isinstance(level, int):
        try:
            level = getattr(logging, level)
        except AttributeError:
            raise ValueError("unsupported literal log level: %s" % level)
        logger.setLevel(level)
    if filename:
        handler = logging.FileHandler(filename, mode=mode)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def make_parser():
    parser = argparse.ArgumentParser(description='Prepare file for pipeline')

    parser.add_argument('--input_file', '-i', type=str, required=True,
                        help='yaml input file')

    parser.add_argument('--folder', '-w', metavar="PATH", required=True,
                        help="destination folder for merged fastq files")

    parser.add_argument('--project_name', '-p', metavar="PATH",
                        help="Project name for config rename")

    parser.add_argument('--merge_only', '-mo', action='store_true',
                        help="Merge fastq files without generating a configfile (Default: FALSE)")

    parser.add_argument('--config_only', '-co', action='store_true',
                        help="Generate the configfile without merging fastq files (Default: FALSE)")

    parser.add_argument('--force', action='store_true',
                        help="Write merged fastq files in the directory even if it exists (Default: FALSE)")

    parser.add_argument('--paired', action='store_true',
                        help="Activate paired end mode (Default: FALSE)")

    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    # Initializing logger
    logger = get_logger('main', level='INFO')
    workflow = App(args=args, logger=logger)

    workflow.run()


if __name__ == '__main__':
    main(sys.argv[1:])
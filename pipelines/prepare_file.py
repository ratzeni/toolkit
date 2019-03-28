#!/usr/bin/env python

import sys
import os
import csv
import itertools
import operator
import argparse
import logging


class App(object):
    def __init__(self, args=None, logger=None):
        self.logger = logger
        
        self.i_filename = args.input_file
        self.ped_filename = args.ped_file if args.ped_file else os.path.join(os.path.dirname(self.i_filename), "project.ped")
        self.reheader_filename = args.reheader_file if args.reheader_file else os.path.join(os.path.dirname(self.i_filename), "reheader.txt")
        self.sets_filename = args.sets_file if args.sets_file else os.path.join(os.path.dirname(self.i_filename), "sets.tsv")

    def get_input_data(self, filename):
        self.logger.info("Reading {}".format(self.i_filename))
        reader = csv.DictReader(open(filename, newline=''), delimiter='\t')
        return [row for row in reader]

    def groupby(self, dic_list, key):
        ret = list()
        for key, items in itertools.groupby(dic_list, operator.itemgetter(key)):
            ret.append(list(items))
        return ret

    def get_parent_id(self, family, person, relationship):
        if person.get('Family_relationship').lower() != "proband":
            return '0'
        else:
            return next(item['Bika_id'] for item in family if item["Family_relationship"].lower() in relationship)

    def create_ped(self, ped_filename, input_data):
        self.logger.info("Creating ped file {}".format(ped_filename))
        ped = csv.DictWriter(open(ped_filename, 'w'),delimiter=' ',fieldnames=['fam_id', 'sample_id', 'father_id','mother_id', 'gender', 'status'])
        ped_list = list()
        families = self.groupby(input_data, 'Family_id')
        for family in families:
            for p in family:
                ped_dict= dict(
                    fam_id="{}_{}".format(p.get('Project'), p.get('Family_id')),
                    sample_id=p.get('Bika_id'),
                    father_id=self.get_parent_id(family, p, 'father'),
                    mother_id=self.get_parent_id(family, p, 'mother'),
                    gender='1' if p.get('Gender') == 'M' else '2',
                    status='1' if p.get('Affected_or_not') == 'no' else '2',
                )
                ped.writerow(ped_dict)
                ped_list.append(ped_dict)
        return ped_list

    def create_reheader(self, reheader_filename, input_data):
        self.logger.info("Creating reheader file {}".format(reheader_filename))
        reheader = csv.DictWriter(open(reheader_filename, 'w'),delimiter=' ',fieldnames=['bika_id', 'client_id'])
        reheader_list = list()
        reheader.writeheader()
        for row in input_data:
            reheader_dict = dict(bika_id=row.get('Bika_id'),client_id=row.get('Client_id').replace(" ","_"))
            reheader.writerow(reheader_dict)
            reheader_list.append(reheader_dict)
        return reheader_list

    def create_sets(self, sets_filename, ped_data):
        self.logger.info("Creating sets file {}".format(sets_filename))
        families = self.groupby(ped_data, 'fam_id')
        with open(sets_filename, 'w') as h:
            h.write("set\tsample\n")
            for family in families:
                fam_id = family[0].get('fam_id')
                ids = [ person.get('sample_id') for person in family ]
                h.write("{}\t{}\n".format(fam_id, ",".join(ids)))

    def run(self):
        input_data = self.get_input_data(filename=self.i_filename)

        ped = self.create_ped(ped_filename=self.ped_filename,input_data=input_data)

        reheader = self.create_reheader(reheader_filename=self.reheader_filename, input_data=input_data)

        self.create_sets(sets_filename=self.sets_filename, ped_data=ped)


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
                        help='tsv input file')

    parser.add_argument('--ped_file', '-p', metavar="PATH",
                        help="ped file (output)")
    
    parser.add_argument('--reheader_file', '-r', metavar="PATH",
                        help="reheader file (output)")

    parser.add_argument('--sets_file', '-s', metavar="PATH",
                        help="set files (output")

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

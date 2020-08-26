#!/usr/bin/env python3

# __description__ = """
# This is an attempt to convert the organism make files to a snakemake pipeline
# """.format(version=__version__)

import argparse
import subprocess
import yaml
import sys
import os
from glob import glob

def parse_args(defaults={"configfile": None, "blacklist": None, "maxjobs": 5, "chrmap": None}):

    parser = argparse.ArgumentParser(prog=sys.argv[0])
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')


    #required
    required.add_argument("--genomeURL", # borrowed from createIndices
                          required=True,
                          help="URL or local path to where the genome fasta file "
                               "is located. The file may optionally be gzipped.")

    required.add_argument("--gtfURLs", # borrowed from createIndices, here is going to be a tuple!
                          nargs="+",
                          help="URL or local path to where the genome annotation "
                               "in GTF format is located. GFF is NOT supported. "
                               "The file may optionally be gzipped. If this file "
                               "is not specified, then RNA-seq related tools will "
                               "NOT be usable. Several URL can be provided.")

    required.add_argument("--gtfNames", # e.g. xx yy ...
                          nargs="+",
                          help="A list of names for the given annotation files")

    required.add_argument("-o", # this is going to be the /data/repository/organism/the_organism
                        "--outdir",
                        dest = "outdir",
                        help="Output directory")

    # Optional
    optional.add_argument("--blacklist", # borrowed from createIndices
                          help="An optional URL or local path to a file to use to "
                               "blacklist regions (such as that provided by the "
                               "ENCODE consortium).",
                               default=defaults["blacklist"])
    optional.add_argument("-c", "--configfile",
                        help="configuration file: config.yaml (default: '%(default)s')",
                        default=defaults["configfile"])

    optional.add_argument("--maxjobs",
                        type=int,
                        default=defaults["maxjobs"],
                        help="Maximum number of jobs to run at the same time. The default is 5.")

    optional.add_argument("--chrmap",
                        default=defaults["chrmap"],
                        help="Mapping file which comes from Devon's github.")

    return parser


def main():
    ## defaults
    this_script_dir = os.path.dirname(os.path.realpath(__file__))

    ## get command line arguments
    parser = parse_args()
    args = parser.parse_args()

    update_anno = False
    if os.path.exists(args.outdir):
        print("\nWarning! Output directory already exists! "
               "The program tries to update the annotation.({})\n".format(args.outdir))
        if os.path.exists(os.path.join(args.outdir,"genome_fasta")):
            update_anno = False # TODO this is not good and need to be changed! We might need a flag to check if there was already a successful run on this organism

    else:
        os.makedirs(args.outdir, exist_ok=False)
        os.makedirs(os.path.join(args.outdir,"Ensembl"), exist_ok = False)
        os.makedirs(os.path.join(args.outdir,"BismarkIndex"), exist_ok = False)
        os.makedirs(os.path.join(args.outdir,"NovoalignMethylIndex"), exist_ok = False)
        os.makedirs(os.path.join(args.outdir,"NovoalignIndex"), exist_ok = False)
    args.outdir = os.path.abspath(args.outdir)

    if args.configfile and not os.path.exists(args.configfile):
        sys.exit("\nError! Provided configfile (-c) not found! ({})\n".format(args.configfile))

    # overwrite the default config by args and given configfile
    if not args.configfile:
        args.configfile = os.path.join(this_script_dir, "config.yaml")

    f = open(args.configfile)
    cf = yaml.load(f)
    f.close()
    for key in cf.keys():
        if key in vars(args):
            cf[key] = vars(args)[key]

    # Set extra params:
    cf['update_anno'] = update_anno
    cf['genome'] = "tmp_genome"

    params = "--jobmode slurm --disable-ui --nopreflight --maxjobs {}".format(args.maxjobs)

    cf['params'] = params
    f = open(os.path.join(args.outdir, "config.yaml"), "w")
    yaml.dump(cf, f, default_flow_style=False)
    f.close()

    module_load_cmd = "module load snakemake/5.16.0 slurm &&".split()
    snakemake_cmd = """
        snakemake --latency-wait {latency_wait} -s {snakefile} --jobs 5 -p --verbose
        --directory {workingdir} --configfile {configfile}
        """.format(latency_wait = cf["snakemake_latency_wait"],
                   snakefile = os.path.join(this_script_dir, "SnakeFile"),
                   workingdir = args.outdir,
                   configfile = os.path.join(args.outdir, 'config.yaml')).split()

    cmd = " ".join(module_load_cmd + snakemake_cmd)
    p = subprocess.Popen(cmd, shell=True)
    try:
        p.wait()
    except:
        print("\nWARNING: Snakemake terminated!!!")
        if p.returncode != 0:
            print("Returncode:", p.returncode)

            # kill snakemake and child processes
            subprocess.call(["pkill", "-SIGTERM", "-P", str(p.pid)])
            print("SIGTERM sent to PID:", p.pid)

if __name__ == "__main__":
    main()

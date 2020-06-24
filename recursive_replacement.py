import argparse
import os
import shlex
import subprocess
from file_replace_string import (
    replace_string_in_file,
    replace_string_in_bam,
    replace_string,
    read_string_replacement_file
)
import logging
import sys
from multiprocess_handling import dumb_scheduler


def main():
    args = parse_args()
    sourcedir = args.sourcedir
    outdir = args.outdir
    replacement_file = args.replacement_file
    ignore_ext = args.ignore_extension
    logfile = args.logfile
    start_log(log=logfile)

    replacement_dict = read_string_replacement_file(replacement_file)
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    do_multiprocess = (args.multiprocessing >= 0)

    cmd_params = []
    for root, directories, files in os.walk(sourcedir):
        print(f"\nRoot directory is {root}.")
        new_outdir = os.path.join(outdir, replace_string(os.path.relpath(root, start=sourcedir), replacement_dict))
        if not os.path.isdir(new_outdir):
            os.mkdir(new_outdir)
        for filename in files:
            print(f"Processing {filename}")
            outfilename = replace_string(filename, replacement_dict)
            infilepath = os.path.join(root, filename)
            outfilepath = os.path.join(new_outdir, outfilename)
            if filename.endswith(".bam"):
                cmd = f"replace_string --infilepath {infilepath} --outfilepath {outfilepath} " \
                      f"--replacement_file {replacement_file}  --num_thread 1"
                cmd_params.append((cmd, None, None))
                if not do_multiprocess and not args.dry_run:
                    replace_string_in_bam(infilepath, outfilepath, replacement_dict)
            elif any([filename.endswith(ending) for ending in ignore_ext]):
                if args.use_symlink:
                    cmd = f"ln -s {infilepath} {outfilepath}"
                else:
                    cmd = f"cp {infilepath} {outfilepath}"
                cmd_params.append((cmd, None, None))
                if not do_multiprocess and not args.dry_run:
                    return_code = subprocess.call(shlex.split(cmd))
                    assert return_code == 0
            else:
                cmd = f"replace_string --infilepath {infilepath} --outfilepath {outfilepath} " \
                      f"--replacement_file {replacement_file}  --num_thread 1"
                cmd_params.append((cmd, None, None))
                if not do_multiprocess and not args.dry_run:
                    replace_string_in_file(infilepath, outfilepath, replacement_dict)
    log_output = '\n'.join([a for a, b, c, in cmd_params])
    logging.info(f"Commands to run: \n{log_output}")
    if do_multiprocess and not args.dry_run:
        dumb_scheduler(cmd_params, max_process_num=args.multiprocessing, polling_period=15)
    return


def start_log(log="stderr", level=logging.DEBUG):
    """
    Initiate program logging. If no log file is specified,
    then log output goes to the default log file: stdout
    """
    if log.lower() == "stdout":
        log = sys.stdout
    elif log.lower() == "stderr":
        log = sys.stderr
    else:
        log = open(log, 'w')
    logging.basicConfig(stream=log,
                        level=level,
                        filemode='w',
                        format="%(asctime)s %(message)s",
                        datefmt="[%m/%d/%Y %H:%M:%S] ")
    logging.info("Program started")
    logging.info("Command line: {0}\n".format(' '.join(sys.argv)))
    return


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replace occurrence of a set of strings with specified values in genomic files and their filenames."
    )
    parser.add_argument('--sourcedir', metavar="PATH", type=str, required=True,
                        help='Path to input directory.')
    parser.add_argument('--outdir', metavar="PATH", type=str, required=True,
                        help='Path to output directory. Existing files will be overwritten.')
    parser.add_argument('--replacement_file', metavar="PATH", type=str, required=True,
                        help="Path to a 2 column TSV-file containing the original string in the first column "
                             "and their corresponding replacement string in the second column.")
    parser.add_argument('--ignore_extension', metavar="EXT", type=str, required=False, nargs='*',
                        help="The set of file extension to ignore. Any files with this extension in `sourcedir` will "
                             "have their filename changed but their content untouched.")
    parser.add_argument('--multiprocessing', metavar="THREADS", type=int, required=False,
                        default=0,
                        help="Number of processes to spawn to run the program. "
                             "Default to not using any multiprocessing.")
    parser.add_argument('--use_symlink', action="store_true",
                        help="If specified, any file specified in --ignore_extension "
                             "will be symlinked instead of copied.")
    parser.add_argument('--dry_run', action="store_true",
                        help="If specified, only output the command to run but not actually run them.")
    parser.add_argument('--logfile', metavar="FILENAME", type=str, required=False,
                        default="stderr",
                        help="Log file. Default: stderr")
    return parser.parse_args()


if __name__ == "__main__":
    main()

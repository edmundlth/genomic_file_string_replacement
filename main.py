import argparse
import os
import shlex
import subprocess
from file_replace_string import (
    read_string_replacement_file
)
from generate_commands import (
    generate_commands,
    rand_string,
    generate_new_filename,
    fastq_cmd,
    bam_cmd,
    textfile_cmd
)
from multiprocess_handling import dumb_scheduler, cmd_runner
import logging
import sys
import pandas as pd
from multiprocessing import Pool

######
# CLI
######
CLI = argparse.ArgumentParser(
    description="Description: Perform string replacement in various genomics file types. \nAvailable subcommands:\n",
    formatter_class=argparse.RawDescriptionHelpFormatter
)
SUBPARSER = CLI.add_subparsers(dest="subcommand")


def subcommand(args=()):
    def decorator(func):
        parser = SUBPARSER.add_parser(func.__name__, description=func.__doc__)
        CLI.description += f"  {func.__name__}\n"
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)

    return decorator


def argument(*name_or_flags, **kwargs):
    return [*name_or_flags], kwargs


PREPARE_ARGS = [
    argument('--sourcedir', metavar="PATH", type=str, required=True,
             help='Path to input directory.'),
    argument('--source_filelist', metavar="PATH", type=str, required=False,
             help='A file containing the list of file paths relative to --sourcedir to be processed. '
                  'If not specified, all files in --sourcedir will be considered.'),
    argument('--outdir', metavar="PATH", type=str, required=True,
             help='Path to output directory. Existing files will be overwritten.'),
    argument('--replacement_file', metavar="PATH", type=str, required=True,
             help="Path to a 2 column TSV-file containing the original string in the first column "
                  "and their corresponding replacement string in the second column."),
    argument('--fastq_filename_fields', type=int, nargs="*", required=False,
             help="A list of zero-based indices to "
                  "specify which `_`-separated fields need to be randomised."),
    argument('--include_only_ext', metavar="EXT", type=str, required=False, nargs='*',
             default=None,
             help="The set of file extension to include. All other extensions will be ignored. "
                  "If not specified, all files in `sourcedir` will be included. "
                  "Files specified by --ignore_extension will still be excluded."),
    argument('--ignore_extension', metavar="EXT", type=str, required=False, nargs='*',
             help="The set of file extension to ignore. "
                  "Any files with this extension in `sourcedir` will "
                  "have their filename changed but their content untouched.")
]

OUTPUT_COMMAND_ARGS = [
    argument('--fileinfo', metavar="PATH", type=str, required=True,
             help='Path to tsv-file of table containing information needed to determine operation to perform on '
                  'each files.'),
    argument('--outdir', metavar="PATH", type=str, required=True,
             help='An output directory to be used in the list of generated commands. '),
    argument('--outfilepath', metavar="PATH", type=str, required=True,
             help='Path to output file where generated commands end up.'),
    argument('--generate_md5', action="store_true",
             help="If specified, file with content changed will get their md5 generated."),
    argument('--anon_batch', action="store_true",
             help="If specified, the directory name of the batch will be anonymised."),
    argument('--use_symlink', action="store_true",
             help="If specified, any file specified in --ignore_extension "
                  "will be symlinked instead of copied."),
    argument('--anon_strlength', metavar="LENGTH", type=int, required=False,
             default=16,
             help='Length of anonymised ID. Default: 16'),
]

RUN_COMMAND_ARGS = [
    argument('--commandfilepath', metavar="PATH", type=str, required=True,
             help="The file path to the file containing a list of unix commands generated by `prepare` to run."),
    argument('--multiprocessing', metavar="THREADS", type=int, required=False,
             default=0,
             help="Number of processes to spawn to run the program. "
                  "Default to not using any multiprocessing."),
    argument('--polling_period', type=int, required=False,
             default=15,
             help="Polling period in seconds for --multiprocessing poller. Default: 15 seconds."),
    argument('--logfile', metavar="PATH", type=str, required=False,
             default="stderr",
             help="A file to store log outputs.")
]


#############
# Subcommands
#############
@subcommand(PREPARE_ARGS)
def prepare(args):
    """
    Parsing a data directory and output a tsv-file of file information in preparation for the `output_command` stage.
    """
    # Parse string replacement file into a dictionary.
    replacement_file = args.replacement_file
    replacement_dict = read_string_replacement_file(replacement_file)

    # Determine the list of files to process.
    sourcedir = args.sourcedir
    if args.source_filelist is not None:
        source_filelist = [os.path.realpath(os.path.join(sourcedir, filename)) for filename in args.source_filelist]
    else:
        source_filelist = None

    # Making sure output directory exist.
    outdir = args.outdir
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

    # Loop through the files
    cmd_params = generate_commands(
        sourcedir,
        source_filelist,
        outdir,
        replacement_file,
        replacement_dict,
        args.ignore_ext,
        args.include_only_ext,
        args.use_symlink
    )

    # Now we have all the commands we want to run in a list. We first write them out to a file:
    cmd_outfilepath = os.path.join("command_list.sh")
    with open(cmd_outfilepath, 'w') as cmd_outfile:
        cmd_outfile.writelines('\n'.join([a for a, b, c, in cmd_params]))
    return


@subcommand(OUTPUT_COMMAND_ARGS)
def output_command(args):
    """
    Read in a tsv-file of file information including filepath, filetype, batch, sample_id,
    output a list of anonymisation commands for each of them.
    """
    use_symlink = args.use_symlink
    anon_batch = args.anon_batch
    df_fileinfo = pd.read_csv(args.fileinfo, sep='\t')
    outdirpath = os.path.abspath(args.outdir)
    replacement_dict = {
        s: rand_string(n=args.anon_strlength) for s in set(df_fileinfo["sample_id"])
    }
    batch_mapping = {
        (sample_id, batch): rand_string(n=args.anon_strlength)
        for sample_id, batch in df_fileinfo[["sample_id", "batch"]].values
    }

    cmd_list = []
    for i, row in df_fileinfo.iterrows():
        filetype = row["filetype"]
        batch = row["batch"]
        sample_id = row["sample_id"]
        flagship = row['flagship']
        if anon_batch:
            batch = batch_mapping[(sample_id, batch)]
        infilepath = row["filepath"]  # os.path.join(datadir, batch, filename)
        directory, filename = os.path.split(infilepath)
        string_map = {sample_id: replacement_dict[sample_id]}  # definitely anonymise sample_id
        if "replacements" in row.keys():  # if there are other replacement to make. Can overwrite sample id replacement
            for pair in row["replacements"].split(','):
                key, val = pair.split(':')
                string_map[key] = val
        cmd = ""
        batch_dir = os.path.join(outdirpath, flagship, batch)
        if not os.path.isdir(batch_dir):  # if the directory doesn't exist, tag on the mkdir command.
            cmd += f"mkdir -p {batch_dir}; "

        if filetype == "fastq":
            outfilename = generate_new_filename(filename, replacement_dict=string_map, remove_fields=range(1, 4))
            outfilepath = os.path.join(batch_dir, outfilename)
            cmd += fastq_cmd(infilepath, outfilepath, use_symlink=use_symlink)
        elif filetype == "bam":
            outfilename = generate_new_filename(filename, replacement_dict=string_map)
            outfilepath = os.path.join(batch_dir, outfilename)
            cmd += bam_cmd(infilepath, outfilepath, string_map, num_thread=1)
        elif filetype == "vcf":
            outfilename = generate_new_filename(filename, replacement_dict=string_map)
            outfilepath = os.path.join(batch_dir, outfilename)
            cmd += textfile_cmd(infilepath, outfilepath, string_map, is_gzip=infilepath.endswith(".gz"))
        else:
            outfilename = generate_new_filename(filename, replacement_dict=string_map)
            outfilepath = os.path.join(batch_dir, outfilename)
            cmd += textfile_cmd(infilepath, outfilepath, string_map, is_gzip=infilepath.endswith(".gz"))

        # if specified, tag on `md5sum` command to the output file.
        if args.generate_md5 and filetype != "fastq":
            cmd += f" ; md5sum {outfilepath} > {outfilepath.strip()}.md5"
        cmd_list.append(cmd)

    with open(args.outfilepath, 'w') as outfile:
        for line in cmd_list:
            outfile.write(line + '\n')
    return


@subcommand(RUN_COMMAND_ARGS)
def run_command(args):
    """
    Run a list of shell commands in multiprocessing mode.
    """
    cmd_params = []
    with open(args.commandfilepath) as infile:
        for line in infile:
            line = line.strip()
            if line:
                cmd_params.append((line, None, None))
    start_log(log=args.logfile)
    do_multiprocess = (args.multiprocessing > 1)
    if not do_multiprocess:
        for cmd, stdin, stdout in cmd_params:
            return_code = subprocess.call(shlex.split(cmd))
            assert return_code == 0, f"Command return with nonzero return code: {cmd}"
    else:
        result = []
        with Pool(processes=args.multiprocessing) as pool:
            mp = pool.map_async(
                worker,
                cmd_params,
                callback=lambda x: result.extend(x)
            )
            mp.wait()
        for cmd, rc in result:
            if rc != 0:
                logging.error(f"Command: `{cmd}` return with non-zero return code: {rc}")

    # # Time to run them using a scheduler if --multiprocessing is set, otherwise we just call and check return code.
    # if not do_multiprocess:
    #     for cmd, stdin, stdout in cmd_params:
    #         return_code = subprocess.call(shlex.split(cmd))
    #         assert return_code == 0, f"Command return with nonzero return code: {cmd}"
    # else:  # do_multiprocess == True here..
    #     dumb_scheduler(cmd_params, max_process_num=args.multiprocessing, polling_period=args.polling_period)
    return


def worker(param):
    proc_rc = subprocess.call(param[0], shell=True)
    return param[0], proc_rc


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


if __name__ == "__main__":
    cli_args = CLI.parse_args()
    if cli_args.subcommand is None:
        CLI.print_help()
    else:
        cli_args.func(cli_args)

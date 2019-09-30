import argparse
import os
import shlex
import subprocess
from .file_replace_string import (
    replace_string_in_file,
    replace_string_in_bam,
    replace_string,
    read_string_replacement_file
)


def main():
    args = parse_args()
    sourcedir = args.sourcedir
    outdir = args.outdir
    replacement_file = args.replacement_file
    ignore_ext = args.ignore_extension

    replacement_dict = read_string_replacement_file(replacement_file)
    if not os.path.isdir(outdir):
        os.mkdir(outdir)

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
                replace_string_in_bam(infilepath, outfilepath, replacement_dict)
            elif any([filename.endswith(ending) for ending in ignore_ext]):
                cmd = f"cp {infilepath} {outfilepath}"
                return_code = subprocess.call(shlex.split(cmd))
                print(f"Return code for `{cmd}` is {return_code}.")
            else:
                replace_string_in_file(infilepath, outfilepath, replacement_dict)
    return None


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
    return parser.parse_args()


if __name__ == "__main__":
    main()

import gzip
import os
from Bio.bgzf import BgzfWriter
from typing import TextIO, Optional
import subprocess
import warnings
import argparse


def main():
    """
    Orchestrate the program execution.
    """
    args = parse_args()
    infilepath = args.infilepath
    outfilepath = args.outfilepath
    old_string = args.old_string
    new_string = args.new_string
    replacement_file = args.replacement_file

    if replacement_file is not None:
        assert old_string is None
        assert new_string is None
        replacement_dict = read_string_replacement_file(replacement_file, sep='\t')
    else:
        replacement_dict = {old_string: new_string}

    if os.path.basename(infilepath).endswith(".bam"):
        replace_string_in_bam(infilepath, outfilepath, replacement_dict, num_thread=args.num_thread)
    else:
        replace_string_in_file(infilepath, outfilepath, replacement_dict)
    return


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replace occurrence of a set of strings with specified values in genomic files and their filenames."
    )
    parser.add_argument('--infilepath', metavar="PATH", type=str, required=True,
                        help='Path to input file.')
    parser.add_argument('--outfilepath', metavar="PATH", type=str, required=True,
                        help='Path to output file. Existing file will be overwritten.')
    parser.add_argument('--old_string', metavar="STRING", type=str, required=False,
                        help="The old string to be replaced by value of --new_string.")
    parser.add_argument('--new_string', metavar="STRING", type=str, required=False,
                        help="The new string to replace value of --old_string.")
    parser.add_argument('--replacement_file', metavar="PATH", type=str, required=False,
                        help="Path to a 2 column TSV-file containing the original string in the first column "
                             "and their corresponding replacement string in the second column. "
                             "Only specify this if --old_string and --new_strings are not specified.")
    parser.add_argument('--num_thread', type=int, required=False, default=4,
                        help="Number of thread to use in samtools.")
    return parser.parse_args()


########################################################################################################
# Wrapper functions
########################################################################################################


def replace_string_in_file(infilename, outfilename, replacement_dict):
    """
    Handle's the string replacement over entire file.

    :param infilename: str
    Path to input file.

    :param outfilename: str
    Path to output file.

    :param replacement_dict: dict
    String mapping

    :return: None
    """
    compression = "bgzip" if "gz" in outfilename.lower() else None
    with infile_handler(infilename) as infile, outfile_handler(outfilename, compression=compression) as outfile:
        for line in infile:
            outfile.write(replace_string(line, replacement_dict))
    return


def replace_string_in_bam(inbam_name, outbam_name, replacement_dict, num_thread=4):
    """
    Same as `replace_string_in_file` function except we needed samtools to do the (de)compression.

    :param inbam_name: String
    The path to the input BAM file

    :param outbam_name: String
    The path to the output BAM file

    :param replacement_dict: dict
    String mapping

    :param num_thread
    Number of threads

    :return: None
    """
    sed_cmd_string = ' | '.join([f"sed 's/{key}/{val}/g'" for key, val in replacement_dict.items()])
    cmd = f"samtools view -h -@ {num_thread} {inbam_name} " \
          f"| {sed_cmd_string} " \
          f"| samtools view -b -h -@ {num_thread} " \
          f"> {outbam_name}"
    if os.path.exists(outbam_name):
        warnings.warn("Overwriting the existing file: %s" % outbam_name)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print(f"STDOUT: {stdout}")
    print(f"STDERR: {stderr}")
    return None


########################################################################################################
# Utilities
########################################################################################################


def read_string_replacement_file(filepath, sep='\t'):
    """
    Read the `replacement_file` which is a 2-column file with columns
    separated by `sep` recording the string replacement map.

    :param filepath: str
     Path to `replacement_file`
    :param sep: chr
    Column separating character.
    :return: dict
    """
    replacement_dict = {}
    with open(filepath) as infile:
        for line in infile:
            key, val = line.strip().split(sep)
            replacement_dict[key] = val
    return replacement_dict


def replace_string(string: str, replacement_dict: dict) -> str:
    """
    Replace all occurrence of the keys of `replacement_dict` in `string` with
    their corresponding values.

    :param string: String
    The string to be processed.

    :param replacement_dict: Dict :: String -> String
    String mapping.

    :return: String
    Return the processed string.
    """
    for key, val in replacement_dict.items():
        string = string.replace(key, val)
    return string


def is_gzip(filepath: str) -> bool:
    """
    Check if a the file specified by filepath
    is a gzipped file. We do this by checking
    whether the first two bytes is the magic
    numbers included in the file header.
    We check them bytewise to avoid issue with
    endianness.

    Note that this wouldn't detect if the gzip
    file is a concatenation of multiple "members".
    See gzip specification at:
     https://www.ietf.org/rfc/rfc1952.txt
    """
    if not os.path.isfile(filepath):
        warnings.warn("The file %s does not exist" % filepath)
        return False

    with open(filepath, 'rb') as filehandle:
        byte1 = filehandle.read(1)
        byte2 = filehandle.read(1)
    if byte1 == b'\x1f' and byte2 == b'\x8b':
        return True
    else:
        return False


def infile_handler(filepath: str) -> TextIO:
    """
    Detect if the file specified by `filepath` is gzip-compressed
    and open the the file in read mode using appriate open handler.
    """
    if is_gzip(filepath):
        return gzip.open(filepath, "rt")
    else:
        return open(filepath, "rt")


def outfile_handler(filepath: str,
                    compression: Optional[str] = None) -> TextIO:
    """
    Return a file handle in write mode using the appropriate
    handle depending on the compression mode.
    Valid compression mode:
        compress = None | "None" | "gzip" | "gz" | "bgzip" | "bgz"
    If compress = None or other input, open the file normally.
    """
    if os.path.isfile(filepath):
        warnings.warn("Overwriting the existing file: %s" % filepath)

    if compression is None:
        return open(filepath, mode="wt")
    elif type(compression) == str:
        if compression.lower() in ["gzip", "gz"]:
            return gzip.open(filepath, mode="wt")
        elif compression.lower() in ["bgzip", "bgz"]:
            return BgzfWriter(filepath)
        elif compression.lower() == "none":
            return open(filepath, mode="wt")
    else:
        raise Exception("`compression = %s` invalid." % str(compression))


if __name__ == "__main__":
    main()

import os
import string
import random
from file_replace_string import (
    replace_string,
)


def generate_commands(
        sourcedir,
        source_filelist,
        outdir,
        replacement_file,
        replacement_dict,
        ignore_ext,
        include_only_ext,
        use_symlink
):
    """
    Loop through the files,
    decide whether to process them,
    decide what command to run on them depending on file type.
    :return:
      A list of (command: str, stdin: None, stdout: None)
    """
    cmd_params = []
    for root, directories, files in os.walk(sourcedir):
        print(f"\nRoot directory is {root}.")
        new_outdir = os.path.join(outdir, replace_string(os.path.relpath(root, start=sourcedir), replacement_dict))
        for filename in files:
            outfilename = replace_string(filename, replacement_dict)
            infilepath = os.path.join(root, filename)
            outfilepath = os.path.join(new_outdir, outfilename)

            # Decide whether to ignore current file.
            has_included_extension = any([filename.endswith(ext) for ext in include_only_ext])
            included_in_filelist = source_filelist is None or os.path.realpath(infilepath) in source_filelist
            if not has_included_extension or not included_in_filelist:
                print(f"Ignoring: {filename}")
                continue
            # Only create the new output directory if it has any files to process that ends up in it.
            if not os.path.isdir(new_outdir):
                os.mkdir(new_outdir)

            # Depending on filetype, we run different commands on them.
            # This is where we start accumulating commandline arguments for each file.
            # We will run them in a scheduler once we have all the commands as a list.
            if filename.endswith(".bam"):
                cmd = f"replace_string --infilepath {infilepath} --outfilepath {outfilepath} " \
                      f"--replacement_file {replacement_file}  --num_thread 1"
                cmd_params.append((cmd, None, None))
            elif any([filename.endswith(ending) for ending in ignore_ext]):
                if use_symlink:
                    cmd = f"ln -s {infilepath} {outfilepath}"
                else:
                    cmd = f"cp {infilepath} {outfilepath}"
                cmd_params.append((cmd, None, None))
            else:
                cmd = f"replace_string --infilepath {infilepath} --outfilepath {outfilepath} " \
                      f"--replacement_file {replacement_file}  --num_thread 1"
                cmd_params.append((cmd, None, None))
    return cmd_params


def rand_string(n=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))


def get_sed_cmd_string(replacement_dict):
    # safely decide on a punctuation sed command separator
    allchar = set(''.join(list(replacement_dict.keys()) + list(replacement_dict.values())))
    sed_separator = None
    for punc in '/' + string.punctuation:  # default to '/'
        if punc not in allchar:
            sed_separator = punc
            break
    if sed_separator is None:
        raise RuntimeError(f"Not safe `sed` separator in the set: {string.punctuation}")

    sed_cmd_string = "sed -e"
    for key, val in replacement_dict.items():
        sed_cmd_string += f" s{sed_separator}{key}{sed_separator}{val}{sed_separator}g "
    return sed_cmd_string


def generate_new_filename(
        infilename,
        replacement_dict=None,
        remove_fields=None,
        filename_sep='_',
        rand_string_len=10,
):
    newfilename = infilename
    if replacement_dict is not None:
        for key, val in replacement_dict.items():
            if val is None:
                val = rand_string(n=rand_string_len)
            newfilename = newfilename.replace(key, val)
    if remove_fields is None:
        remove_fields = []
    newfilename = filename_sep.join(
        [
            field for i, field in enumerate(newfilename.split(filename_sep))
            if (i not in remove_fields) and field]
    )
    return newfilename


def fastq_cmd(infilepath, outfilepath, use_symlink=True):
    if use_symlink:
        cmd = "ln -s"
    else:
        cmd = "cp"
    cmd_string = f"{cmd} {infilepath} {outfilepath}"
    # copy / symlink md5 file over if exist
    md5path_old = f"{infilepath}.md5"
    md5path_new = f"{outfilepath}.md5"
    if os.path.isfile(md5path_old) or os.path.islink(md5path_old):
        cmd_string += f" ; {cmd} {md5path_old} {md5path_new}"
    return cmd_string


def bam_cmd(inbam_path, outbam_path, replacement_dict, num_thread=4, remove_pg=True):
    """
    Same as `replace_string_in_file` function except we needed samtools to do the (de)compression.

    :param inbam_path: String
    The path to the input BAM file

    :param outbam_path: String
    The path to the output BAM file

    :param replacement_dict: dict
    String mapping

    :param num_thread
    Number of threads

    :return: None
    """
    sed_cmd_string = get_sed_cmd_string(replacement_dict)
    edit_cmd = f"samtools view -h -@ {num_thread} {inbam_path} " \
          f"| {sed_cmd_string} " \
          f"| samtools view -b -h -@ {num_thread} "
    if remove_pg:  # if @PG tags should be removed from BAM header
        cmd = f"samtools reheader -P " \
              f"<(samtools view -H {inbam_path} | grep -v '^@PG' | {sed_cmd_string}) " \
              f"<({edit_cmd}) > {outbam_path}"
    else:
        cmd = f"{edit_cmd} > {outbam_path}"
    return cmd


def textfile_cmd(infilepath, outfilepath, replacement_dict, is_gzip=False):
    sed_cmd_string = get_sed_cmd_string(replacement_dict)
    if is_gzip:
        cmd = f"gzip -cd {infilepath} | {sed_cmd_string} | gzip -c > {outfilepath}"
    else:
        cmd = f"{sed_cmd_string} {infilepath} > {outfilepath}"
    return cmd

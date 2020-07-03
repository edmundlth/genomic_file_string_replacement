import shlex
import subprocess
import logging
import time

import numpy as np

COMPLETE = 1
WAITING = -1
RUNNING = 0


########################################################################################################
# Multi-process handling
########################################################################################################


def cmd_runner(cmd, stdin=None, stdout=None, shell=False):
    args = shlex.split(cmd)
    return subprocess.Popen(args, stdin=stdin, stdout=stdout, shell=shell)


def dumb_scheduler(param_list, max_process_num=10, polling_period=0.5):
    num_jobs = len(param_list)
    logging.info(f"Scheduler starting with {num_jobs} jobs.")
    remaining_job_index = list(range(num_jobs - 1, -1, -1))
    job_states = np.full(num_jobs, WAITING, dtype=np.int8)
    processes = []
    while not np.all(job_states == COMPLETE):
        while np.sum(job_states == RUNNING) < max_process_num and WAITING in job_states:
            job_index = remaining_job_index.pop()
            cmd, stdin, stdout = param_list[job_index]
            logging.info(f"Running cmd: {cmd}")
            processes.append((job_index, cmd_runner(cmd, stdin=stdin, stdout=stdout)))
            job_states[job_index] = RUNNING

        # Poll current active processes
        for job_index, proc in processes:
            if job_states[job_index] != COMPLETE:
                poll_state = proc.poll()
                if poll_state is not None:
                    return_code = poll_state
                    cmd, stdin, stdout = param_list[job_index]
                    if return_code != 0:
                        logging.warning(f"WARNING: Command ```{cmd}``` return with nonzero return code: {return_code}.")
                    job_states[job_index] = COMPLETE
                    logging.info(f"\nCommand ```{cmd}``` completed with return code {return_code}.")
                    logging.debug(f"\nNumber of remaining jobs : {np.sum(job_states != COMPLETE)}"
                                  f"\nNumber of completed jobs : {np.sum(job_states == COMPLETE)}"
                                  f"\nNumber of running jobs   : {np.sum(job_states == RUNNING)}")
        time.sleep(polling_period)
    for cmd, stdin, stdout in param_list:
        if stdin is not None:
            stdin.close()
        if stdout is not None:
            stdout.close()
    logging.info("Exiting job scheduler.")
    return processes

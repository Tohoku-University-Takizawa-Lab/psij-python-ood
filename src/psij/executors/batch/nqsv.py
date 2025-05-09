from pathlib import Path
from psij import Job, JobState, JobStatus, SubmitException, JobSpec, ResourceSpecV1
from typing import IO, Optional, List, Dict, Collection, Union, Sequence, Any, cast
from psij.executors.batch.script_generator import TemplatedScriptGenerator
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor
from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutorConfig
from psij.executors.batch.batch_scheduler_executor import check_status_exit_code

import re
import subprocess
from threading import Thread
from datetime import timedelta, datetime

_NQSV_DIR = '/opt/nec/nqsv/bin/'
_QDEL_COMMAND = _NQSV_DIR + 'qdel'
_QSUB_COMMAND = _NQSV_DIR + 'qsub'
_QSTAT_COMMAND = _NQSV_DIR + 'qstat'
_QWAIT_COMMAND = _NQSV_DIR + 'qwait'
_QHOLD_COMMAND = _NQSV_DIR + 'qhold'
_QRLS_COMMAND = _NQSV_DIR + 'qrls'

LARGE_TIMEOUT = timedelta(days=3650)


class _NQSJobWaitingThread(Thread):
    """A thread that waits for a job to finish and updates its status."""

    def __init__(self, job: Job, ex: Any) -> None:
        super().__init__()
        self._job = job
        self._ex = ex

    def run(self) -> None:
        """Wait for the job to finish and update its status."""
        st = self._wait()
        self._ex._set_job_status(self._job, st)

    def _enable_wait_status(self,
                            target_states: Optional[Union[JobState, Sequence[JobState]]] = None) \
            -> bool:
        """Check if the target states are valid for waiting."""
        if target_states is None:
            return True
        if isinstance(target_states, JobState):
            target_states = [target_states]
        # NQSV's qwait command is not support ACTIVE/QUEUED state
        for state1 in target_states:
            if state1 is JobState.ACTIVE or state1 is JobState.QUEUED:
                return False
        return True

    def _parse_wait_output(self, out: str) -> JobStatus:
        """Parse the output of the qwait command."""
        state = JobState.FAILED
        exit_code = None
        if 'exited' in out:
            s = out.split(' ')
            if int(s[1]) == 0:
                state = JobState.COMPLETED
            else:
                state = JobState.FAILED
            exit_code = int(s[1])
        elif 'deleted' in out:
            state = JobState.CANCELED
        elif 'error' in out or 'time out' in out or 'qwait error' in out:
            state = JobState.FAILED
            # killed by signal or rerun or system failure or resource limit exceeded
        else:
            # The job has already finished...
            state = JobState.COMPLETED
        r = JobStatus(state=state, exit_code=exit_code, message=None)
        return r

    def _run_command_using_stderr(self, cmd: List[str]) -> str:
        """Run a command and return the stderr output."""
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return res.stderr

    def _wait(self, timeout: Optional[timedelta] = None,
              target_states: Optional[Union[JobState, Sequence[JobState]]] = None) \
            -> Any:

        # NQSV's qwait command is not support ACTIVE/QUEUED state, then use the orignal wait func.
        if self._enable_wait_status(target_states) is False:
            return self._ex._job_wait(timeout, target_states)

        if timeout:
            command = [_QWAIT_COMMAND, '-w', 'exited', '-t', str(timeout.total_seconds()),
                       str(self._job.native_id)]
        else:
            command = [_QWAIT_COMMAND, '-w', 'exited', str(self._job.native_id)]

        out = self._run_command_using_stderr(command)
        return self._parse_wait_output(out)


class NQSVExecutorConfig(BatchSchedulerExecutorConfig):
    """Configuration for the NQSV executor."""

    pass


class NQSVJobExecutor(BatchSchedulerExecutor):
    """
    An executor for the NEC NQSV batch scheduler.
    This executor uses NQSV to submit jobs. It is
    assumed that NQSV is installed and available in
    the system path. NQSV is a batch job scheduler
    developed by NEC Corporation.
    """

    _STATE_MAP = {
        # short state names
        'QUE': JobState.QUEUED,
        'RUN': JobState.ACTIVE,
        'WAT': JobState.QUEUED,
        'HLD': JobState.QUEUED,
        'SUS': JobState.QUEUED,
        'ARI': JobState.QUEUED,
        'TRS': JobState.QUEUED,
        'EXT': JobState.COMPLETED,
        'PRR': JobState.QUEUED,
        'POR': JobState.COMPLETED,
        'MIG': JobState.QUEUED,
        'STG': JobState.QUEUED,
        # long state names
        'Arriving': JobState.QUEUED,
        'Exiting': JobState.COMPLETED,
        'Held': JobState.HELD,
        'Holding': JobState.QUEUED,
        'Migrating': JobState.QUEUED,
        'Post-running': JobState.COMPLETED,
        'Pre-running': JobState.QUEUED,
        'Queued': JobState.QUEUED,
        'Running': JobState.ACTIVE,
        'Staging': JobState.QUEUED,
        'Suspending': JobState.QUEUED,
        'Transferring': JobState.QUEUED,
        'Waiting': JobState.QUEUED,
    }

    def __init__(self, url: Optional[str] = None, config: Optional[NQSVExecutorConfig] = None):
        """Initialize the NQSV executor."""
        if config is None:
            config = NQSVExecutorConfig()
        super().__init__(url=url, config=config)
        path = Path(__file__).parent / 'nqsv/nqsv.mustache'
        self.generator = TemplatedScriptGenerator(config, path)
        self.submit_frag = False
        self.cancel_frag = False
        self.use_wait_command = False
        self._wait_threads: List[_NQSJobWaitingThread] = []

    # Override submit function.
    def submit(self, job: Job) -> None:
        """Submit a job to the NQSV scheduler."""
        super().submit(job)
        if self.use_wait_command:
            thread = _NQSJobWaitingThread(job, self)
            thread.start()
            self._wait_threads.append(thread)
        return None

    def generate_submit_script(self,
                               job: Job, context: Dict[str, object], submit_file: IO[str]) -> None:
        """Generate a submit script for the NQSV scheduler."""
        self.generator.generate_submit_script(job, context, submit_file)

    def get_submit_command(self, job: Job, submit_file_path: Path) -> List[str]:
        """Get the command to submit a job to the NQSV scheduler."""
        return [_QSUB_COMMAND, str(submit_file_path.absolute())]

    def job_id_from_submit_output(self, out: str) -> str:
        """Extract the job ID from the output of the submit command."""
        self.submit_frag = True
        s = out.strip().split()[1]
        out = ""
        for char in s:
            if char.isdigit():
                out += char
        return out

    def get_cancel_command(self, native_id: str) -> List[str]:
        """Get the command to cancel a job in the NQSV scheduler."""
        self.cancel_frag = True
        return [_QDEL_COMMAND, native_id]

    def process_cancel_command_output(self, exit_code: int, out: str) -> None:
        """See :meth:`~.BatchSchedulerExecutor.process_cancel_command_output`."""
        raise SubmitException('Failed job cancel job: %s' % out)

    def get_status_command(self, native_ids: Collection[str]) -> List[str]:
        """Get the command to check the status of a job in the NQSV scheduler."""
        return [_QSTAT_COMMAND, '-F', 'rid,stt', '-n', '-l'] + list(native_ids)

    def get_hold_command(self, native_id: str) -> List[str]:
        """Get the command to hold a job in the NQSV scheduler."""
        return [_QHOLD_COMMAND, native_id]

    def process_hold_command_output(self, exit_code: int, out: str) -> str:
        """See :meth:`~.BatchSchedulerExecutor.process_hold_command_output`."""
        if exit_code != 0:
            raise SubmitException('Failed job hold: %s' % out)
        return out

    def get_release_command(self, native_id: str) -> List[str]:
        """Get the command to release a job in the NQSV scheduler."""
        return [_QRLS_COMMAND, native_id]

    def process_release_command_output(self, exit_code: int, out: str) -> str:
        """See :meth:`~.BatchSchedulerExecutor.process_release_command_output`."""
        if exit_code != 0:
            raise SubmitException('Failed job release: %s' % out)
        return out

    def get_info_command(self, native_ids: Optional[List[str]] = None,
                         owner: Optional[str] = None) -> List[str]:
        """Get the command to get job information in the NQSV scheduler."""
        cmdline = [_QSTAT_COMMAND, '-f']

        if owner is not None:
            # NQSV needs -Ps(special user privilege) option to get the other job.
            cmdline.extend(['-Ps', '-u', owner])

        if native_ids is not None:
            cmdline.extend(native_ids)

        return cmdline

    def parse_status_output(self, exit_code: int, out: str) -> Dict[str, JobStatus]:
        """Parse the output of the status command."""
        check_status_exit_code('qstat', exit_code, out)
        r = {}
        lines = iter(out.split('\n'))
        for line in lines:
            if not line:
                continue

            cols = line.split()

            if (len(cols) == 8 and self.cancel_frag):
                s = cols[2]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.CANCELED
                r[native_id] = JobStatus(state=state, message=None)

            elif (len(cols) == 8):
                s = cols[1]
                native_id = ""
                for char in s:
                    if char.isdigit():
                        native_id += char
                state = JobState.COMPLETED
                r[native_id] = JobStatus(state=state, message=None)

            else:
                assert len(cols) == 2
                match = re.search(r'\b(\d+)\b', cols[0])
                native_id = cast(str, match.group(1) if match else None)
                native_state = cols[1]
                state = self._get_state(native_state)
                msg = None
                r[native_id] = JobStatus(state=state, message=msg)

        return r

    def _get_state(self, state: str) -> JobState:
        """Convert the state string to a JobState enum."""
        assert state in NQSVJobExecutor._STATE_MAP
        return NQSVJobExecutor._STATE_MAP[state]

    def get_list_command(self) -> List[str]:
        """Get the command to list jobs in the NQSV scheduler."""
        return [_QSTAT_COMMAND, '-F', 'rid', '-n', '-l']

    def parse_list_output(self, out: str) -> List[str]:
        """Parse the output of the list command."""
        r = []
        lines = iter(out.split('\n'))
        for line in lines:
            c = line.split('.')
            r.append(c[0])
        return r

    def parse_info_command_output(self, exit_code: int, out: str,
                                  jobs: Optional[List[Job]] = None) -> List[Job]:
        """Parse the output of the info command."""
        check_status_exit_code(_QSTAT_COMMAND, exit_code, out)

        jobids = out.strip().split("Request ID: ")[1:]
        job_list = []

        for jobid in jobids:
            match = re.search(r"^(.*?)(?=\.)", jobid, re.MULTILINE)
            if match:
                native_id = match.group(1).strip()
            else:
                continue  # Skip if no match

            match = re.search(r"Current State\s+= (.+)", jobid)
            if match:
                current_state = match.group(1).strip() if match else ""
                current_state = self._get_state(current_state)
            else:
                continue  # Skip if no match

            # account -> Account Code
            account = re.search(r"Account Code = (.+)", jobid)
            # job_name = Request Name
            job_name = re.search(r"Request Name = (.+)", jobid)
            # job_owner -> User Name
            job_owner = re.search(r"User  Name = (.+)", jobid)
            # procs -> CPU Number
            procs = re.search(r"CPU Number\s+= Max:\s+(\d+)", jobid)
            # queue_name -> Queue
            queue_name = re.search(r"Queue = (.+?)@", jobid)
            # wallclock_time -> Elapse
            wallclock_time = re.search(r"Elapse\s+= (\d+)S", jobid)
            # wallclock_limit -> Elaspe Time Limit
            wallclock_limit = re.search(r"\(Per-Req\) Elapse Time Limit\s+= Max:\s+(\d+)S", jobid)
            # cpu_time -> CPU Time
            cpu_time = re.search(r"CPU Time\s+= ([\d.]+)S", jobid)
            # submission_time ->  Entered Queue Time
            submission_time = re.search(r"Entered Queue Time\s+= (.+)", jobid)
            # dispatch_time -> Started Request Time
            dispatch_time = re.search(r"Started Request Time\s+= (.+)", jobid)
            # nnodes -> Number of Jobs
            nnode = re.search(r"Number of Jobs\s+= (\d+)", jobid)
            # exectuon_hosts -> Execution Hosts
            p = re.findall(r"Execution Hosts\(JSVNO\):([\s\S]+)\n\s{2,}\w+", jobid, re.MULTILINE)
            if p:
                ehosts = re.findall(r"([\w\d\.]+)\(\w+\)", p[0])
            else:
                ehosts = []

            job = None
            if jobs is not None:
                # Search for the job in the list of jobs
                for j in jobs:
                    if j.native_id == native_id:
                        job = j
                        break
            if job is None:
                spec = JobSpec()
                job = Job(spec=spec)

            if job.executor is None:
                job.executor = self

            if job.spec is None:
                job.spec = JobSpec()

            job._native_id = native_id
            spec = job.spec

            if account is None or account.group(1).strip() == "(none)":
                spec.attributes.account = None
            else:
                spec.attributes.account = account.group(1).strip()

            if procs is None or procs.group(1).strip() == "UNLIMITED":
                procs_str = '0'
            else:
                procs_str = procs.group(1).strip()

            if nnode is None:
                nnode_str = '1'
            else:
                nnode_str = nnode.group(1).strip()

            if spec.resources is None:
                spec.resources = ResourceSpecV1(process_count=int(procs_str) * int(nnode_str))
            elif isinstance(spec.resources, ResourceSpecV1):
                spec.resources.process_count = int(procs_str) * int(nnode_str)

            if wallclock_limit is None or wallclock_limit.group(1).strip() == "UNLIMITED":
                spec.attributes.duration = timedelta(seconds=0)
            else:
                spec.attributes.duration = timedelta(seconds=int(wallclock_limit.group(1).strip()))

            if job_name is not None:
                spec.name = job_name.group(1).strip()
            if wallclock_time is not None:
                job.current_info.wall_time = int(wallclock_time.group(1).strip())
            if queue_name is not None:
                spec.attributes.queue_name = queue_name.group(1).strip()
            job.status.state = current_state
            if submission_time is not None:
                job.current_info.submission_time = \
                    datetime.strptime(submission_time.group(1).strip(),
                                      "%a %b %d %H:%M:%S %Y")
            if dispatch_time is None or dispatch_time.group(1).strip() == "(none)":
                job.current_info.dispatch_time = None
            else:
                job.current_info.dispatch_time = \
                    datetime.strptime(dispatch_time.group(1).strip(), "%a %b %d %H:%M:%S %Y")

            if job_owner is not None:
                job.current_info.owner = job_owner.group(1).strip()

            job.current_info.resourcelist = []
            if len(ehosts) > 0:
                for ehost in ehosts:
                    n = {"name": ehost, "procs": int(procs_str)}
                    job.current_info.resourcelist.append(n)

            if cpu_time is None:
                job.current_info.cpu_time = 0
            else:
                # for example, qstat -f information is "CPU Time  = 5.888000S"
                # but we need to convert it to int.
                job.current_info.cpu_time = int(float(cpu_time.group(1).strip()))
            job.current_info.submit_host = None  # Can not get submit host from qstat
            spec.directory = None  # Can not get directory from qstat

            job_list.append(job)

        return job_list

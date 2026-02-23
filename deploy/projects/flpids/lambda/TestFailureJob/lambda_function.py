"""
TestFailureJob – intentionally raises an exception so that the Step Functions
Catch block fires and routes execution through FinalizeJobOnCatch (FAILURE path).

Use this Lambda as a drop-in replacement for the real transfer Lambda (e.g.
RealJobName) during testing of the failure/catch branch.  The function accepts
the same input payload that TransferAndProcessFile delivers so it can be wired
up without modifying the state machine.
"""
import json
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def lambda_handler(event, context):
    """
    Accept the standard TransferAndProcessFile payload and immediately raise
    an exception to trigger the Step Functions Catch block.

    Expected input (mirrors RealJobName / TransferFile payload):
        jobId           : str
        project         : str
        table_name      : str
        bucket          : str
        secret_id       : str
        verify_tls      : bool
        timeout_seconds : int
        pipeline        : str  (from setRunning.Payload)
        echo_folder     : str
        project_name    : str
        file_pattern    : str
        echo_subfolder  : str
        step            : str
        header          : bool
        to_queue        : bool
        env             : str | None
        lambda_arn      : str
        debug           : bool

    Always raises RuntimeError → Step Functions catches it → FinalizeJobOnCatch runs.
    """
    debug = bool(event.get("debug", False))

    job_id   = event.get("jobId", "<unknown>")
    project  = event.get("project", "<unknown>")
    pipeline = event.get("pipeline", "<unknown>")

    if debug:
        print(f"[DEBUG] TestFailureJob invoked at {_now_iso()}")
        print(f"[DEBUG] jobId={job_id} project={project} pipeline={pipeline}")
        print(f"[DEBUG] full event={json.dumps(event, default=str)}")

    raise RuntimeError(
        f"TestFailureJob: intentional failure for jobId={job_id} "
        f"project={project} pipeline={pipeline} at {_now_iso()}"
    )

import os
import sys
import time
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = os.getenv("DATABRICKS_JOB_ID")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("❌ Error: Missing DATABRICKS_HOST or DATABRICKS_TOKEN.")
    sys.exit(1)

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

def provision_compute(cluster_id: str):
    """Ensure compute resources are active before running jobs."""
    if not cluster_id:
        print("ℹ️ No specific CLUSTER_ID provided. Relying on Job cluster/serverless compute.")
        return

    print(f"🔍 Checking compute status for Cluster ID: {cluster_id}...")
    try:
        cluster = w.clusters.get(cluster_id=cluster_id)
        print(f"Current Cluster State: {cluster.state.value}")

        if cluster.state == State.TERMINATED:
            print("⚡ Starting terminated cluster (Provisioning compute)...")
            w.clusters.start(cluster_id=cluster_id)
            w.clusters.ensure_cluster_is_running(cluster_id=cluster_id)
            print("✅ Compute is up and RUNNING.")
        elif cluster.state == State.RUNNING:
            print("✅ Compute is already RUNNING.")
        elif cluster.state == State.PENDING:
            print("⏳ Waiting for compute initialization...")
            w.clusters.ensure_cluster_is_running(cluster_id=cluster_id)
            print("✅ Compute is up and RUNNING.")
    except Exception as e:
        print(f"⚠️ Warning during cluster provisioning: {e}. Continuing with Job run...")

def trigger_and_monitor_job(job_id: int):
    print(f"🚀 Triggering Databricks Job ID: {job_id}...")
    run = w.jobs.run_now(job_id=job_id)
    run_id = run.run_id
    print(f"✅ Job triggered successfully. Run ID: {run_id}")

    print("⏳ Monitoring run progress via REST API...")
    while True:
        run_status = w.jobs.get_run(run_id=run_id)
        life_cycle_state = run_status.state.life_cycle_state
        result_state = run_status.state.result_state

        status_str = f"Status: {life_cycle_state.value}"
        if result_state:
            status_str += f" | Result: {result_state.value}"
        print(status_str)

        if life_cycle_state.value in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            if result_state and result_state.value == "SUCCESS":
                print("🎉 Job run completed successfully!")
                return 0
            else:
                print(f"❌ Job run failed with result: {result_state}")
                return 1

        time.sleep(10)

if __name__ == "__main__":
    if not JOB_ID:
        print("❌ Error: DATABRICKS_JOB_ID not set.")
        sys.exit(1)

    # 1. Provision / Verify Compute
    if CLUSTER_ID:
        provision_compute(CLUSTER_ID)

    # 2. Run Job and Monitor Status
    exit_code = trigger_and_monitor_job(int(JOB_ID))
    sys.exit(exit_code)
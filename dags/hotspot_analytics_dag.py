"""
hotspot_analytics_dag.py
-------------------------
Airflow DAG for the Food Delivery Hotspot Analytics Pipeline.

Flow:
    setup_window
        → provision_cluster
        → stage_script
        → launch_job
        → job_monitor
        → teardown_cluster
        → [notify_success | notify_failure]
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ── Constants ──────────────────────────────────────────────────────────────────

DAG_ID           = "food_delivery_hotspot_dag"
METADATA_BUCKET  = Variable.get("metadata_bucket",  default_var="your-metadata-bucket")
SLACK_CONN_ID    = Variable.get("slack_conn_id",    default_var="slack_default")
SLACK_ERR_CONN   = Variable.get("slack_err_conn",   default_var="slack_error")
EMR_RELEASE      = Variable.get("emr_release",      default_var="emr-6.3.0")
EMR_SUBNET       = Variable.get("emr_subnet_id",    default_var="subnet-xxxxxxxx")
EC2_KEY_PAIR     = Variable.get("ec2_key_pair",     default_var="your-key-pair")
INSTANCE_TYPE    = Variable.get("emr_instance_type",default_var="m5.4xlarge")
CORE_NODES       = int(Variable.get("emr_core_nodes", default_var="4"))
COLLABORATION_ID = Variable.get("cleanrooms_collaboration_id", default_var="your-collaboration-id")
MEMBERSHIP_ID    = Variable.get("cleanrooms_membership_id",    default_var="your-membership-id")

SCRIPT_PREFIX    = "hotspot-pipeline/scripts"

log = logging.getLogger(__name__)

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_date_window(conf: dict) -> tuple[str, str]:
    if conf.get("start_date") and conf.get("end_date"):
        return conf["start_date"], conf["end_date"]
    end_dt   = datetime.utcnow().date() - timedelta(days=5)
    start_dt = end_dt - timedelta(days=6)
    return str(start_dt), str(end_dt)


def _emr():
    return boto3.client("emr", region_name="us-east-1")

# ── Task functions ─────────────────────────────────────────────────────────────

def setup_window(**context):
    conf = context["dag_run"].conf or {}
    start_date, end_date = _get_date_window(conf)
    log.info("Processing window: %s → %s", start_date, end_date)
    context["ti"].xcom_push(key="start_date", value=start_date)
    context["ti"].xcom_push(key="end_date",   value=end_date)


def provision_cluster(**context):
    client   = _emr()
    response = client.run_job_flow(
        Name=f"hotspot-pipeline-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        ReleaseLabel=EMR_RELEASE,
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
        Instances={
            "InstanceGroups": [
                {"Name": "Primary", "Market": "ON_DEMAND",
                 "InstanceRole": "MASTER", "InstanceType": INSTANCE_TYPE, "InstanceCount": 1},
                {"Name": "Core",    "Market": "ON_DEMAND",
                 "InstanceRole": "CORE",   "InstanceType": INSTANCE_TYPE, "InstanceCount": CORE_NODES},
            ],
            "Ec2KeyName":   EC2_KEY_PAIR,
            "Ec2SubnetId":  EMR_SUBNET,
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        Configurations=[{
            "Classification": "spark-defaults",
            "Properties": {
                "spark.executor.memory":           "12g",
                "spark.driver.memory":             "8g",
                "spark.executor.cores":            "4",
                "spark.sql.shuffle.partitions":    "200",
                "spark.dynamicAllocation.enabled": "true",
            },
        }],
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{METADATA_BUCKET}/emr-logs/",
        Tags=[
            {"Key": "project",    "Value": "food-delivery-hotspot"},
            {"Key": "team",       "Value": "data-engineering"},
            {"Key": "managed-by", "Value": "airflow"},
        ],
        VisibleToAllUsers=True,
    )
    cluster_id = response["JobFlowId"]
    log.info("Cluster created: %s", cluster_id)
    context["ti"].xcom_push(key="cluster_id", value=cluster_id)


def stage_script(**context):
    ti         = context["ti"]
    start_date = ti.xcom_pull(task_ids="setup_window", key="start_date")
    end_date   = ti.xcom_pull(task_ids="setup_window", key="end_date")
    run_prefix = f"hotspot-pipeline/runs/{start_date}_to_{end_date}"
    dest_key   = f"{run_prefix}/hotspot_spark.py"

    s3 = boto3.client("s3")
    s3.copy_object(
        CopySource={"Bucket": METADATA_BUCKET, "Key": f"{SCRIPT_PREFIX}/hotspot_spark.py"},
        Bucket=METADATA_BUCKET,
        Key=dest_key,
    )
    staged = f"s3://{METADATA_BUCKET}/{dest_key}"
    log.info("Script staged → %s", staged)
    ti.xcom_push(key="staged_script", value=staged)


def launch_job(**context):
    ti            = context["ti"]
    cluster_id    = ti.xcom_pull(task_ids="provision_cluster", key="cluster_id")
    staged_script = ti.xcom_pull(task_ids="stage_script",      key="staged_script")
    start_date    = ti.xcom_pull(task_ids="setup_window",       key="start_date")
    end_date      = ti.xcom_pull(task_ids="setup_window",       key="end_date")

    client   = _emr()
    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": "hotspot-spark",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "--conf", "spark.yarn.submit.waitAppCompletion=true",
                    staged_script,
                    "--start-date",       start_date,
                    "--end-date",         end_date,
                    "--metadata-bucket",  METADATA_BUCKET,
                    "--collaboration-id", COLLABORATION_ID,
                    "--membership-id",    MEMBERSHIP_ID,
                ],
            },
        }],
    )
    step_id = response["StepIds"][0]
    log.info("Step submitted: %s", step_id)
    ti.xcom_push(key="step_id", value=step_id)


def job_monitor(**context):
    import time
    ti         = context["ti"]
    cluster_id = ti.xcom_pull(task_ids="provision_cluster", key="cluster_id")
    step_id    = ti.xcom_pull(task_ids="launch_job",        key="step_id")

    client   = _emr()
    wait     = 30
    max_wait = 300
    timeout  = 5 * 3600
    elapsed  = 0
    terminal = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}

    while elapsed < timeout:
        state = client.describe_step(
            ClusterId=cluster_id, StepId=step_id
        )["Step"]["Status"]["State"]
        log.info("Step %s — state: %s (%ds)", step_id, state, elapsed)
        if state in terminal:
            if state != "COMPLETED":
                raise RuntimeError(f"Step {step_id} ended with: {state}")
            return
        time.sleep(wait)
        elapsed += wait
        wait     = min(wait * 2, max_wait)

    raise TimeoutError(f"Step {step_id} timed out after 5 hours.")


def teardown_cluster(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="provision_cluster", key="cluster_id")
    if cluster_id:
        _emr().terminate_job_flows(JobFlowIds=[cluster_id])
        log.info("Cluster %s terminated.", cluster_id)

# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Food Delivery Hotspot Analytics — weekly rolling window via Amazon Clean Rooms",
    schedule_interval="0 5 * * 1",   # every Monday 05:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hotspot", "food-delivery", "clean-rooms", "location-analytics", "pyspark"],
) as dag:

    t_setup     = PythonOperator(task_id="setup_window",     python_callable=setup_window)
    t_provision = PythonOperator(task_id="provision_cluster",python_callable=provision_cluster)
    t_stage     = PythonOperator(task_id="stage_script",     python_callable=stage_script)
    t_launch    = PythonOperator(task_id="launch_job",       python_callable=launch_job)
    t_monitor   = PythonOperator(task_id="job_monitor",      python_callable=job_monitor)
    t_teardown  = PythonOperator(task_id="teardown_cluster", python_callable=teardown_cluster,
                                 trigger_rule="all_done")

    t_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=":white_check_mark: *Food Delivery Hotspot Pipeline* completed successfully. 🍕",
        trigger_rule="all_success",
    )
    t_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_ERR_CONN,
        message=":red_circle: *Food Delivery Hotspot Pipeline* FAILED — check Airflow logs.",
        trigger_rule="one_failed",
    )

    (
        t_setup
        >> t_provision
        >> t_stage
        >> t_launch
        >> t_monitor
        >> t_teardown
        >> [t_success, t_failure]
    )

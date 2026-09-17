# Python model on a Unity Catalog target.
# dbt.config() only accepts Python literals, so the submission method is hardcoded.
# serverless_cluster needs no cluster at all; all_purpose_cluster / job_cluster /
# workflow_job are the alternatives when you have one.


def model(dbt, session):
    dbt.config(materialized="table", submission_method="serverless_cluster")
    return dbt.ref("stg_customers").select("id", "name")

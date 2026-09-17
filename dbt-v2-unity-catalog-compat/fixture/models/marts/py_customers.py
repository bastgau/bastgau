# Python model on a Unity Catalog target. Execution needs a cluster or a job, which a
# SQL-warehouse-only workspace does not provide, so the live phase runs it only when
# DBT_PYTHON_MODEL is set. Parsing and node resolution are covered offline.
# dbt.config() only accepts Python literals, so the submission method is hardcoded:
# change it here (all_purpose_cluster / job_cluster / serverless_cluster) if needed.


def model(dbt, session):
    dbt.config(materialized="table", submission_method="all_purpose_cluster")
    return dbt.ref("stg_customers").select("id", "name")

import argparse
import json
import logging
import multiprocessing
from datetime import datetime
from multiprocessing import get_logger, log_to_stderr

import pandas as pd

import datamigration_wrapper as dms


    
def func(a, b, c, d, e):
    migration_job = dms.DataMigrationService(a, b, c, d, e)
    migration_job.generate_migration_job()
"""
def func(migration_job):
    migration_job.generate_migration_job()
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--migration-file", type=str)
    args = parser.parse_args("")

    migration_file = args.migration_file or "Test GCP Migration.csv"
    df_machine_types = pd.read_csv("machine_types.csv")
    df_migration_data = pd.read_csv(migration_file)

    # print(df_migration_data)

    project_id = "aws-rds-gcp-cloudsql"
    region_id = "us-east4"
    prefix_cp_source = "auto-cp-pg-"
    prefix_cp_cloudsql = "auto-cs-pg-"
    prefix_mj = "auto-mj-"

    list_migration_job = []
    for index, row in df_migration_data.iterrows():
        dc = {}
        dc["source_connection"] = {
            "postgresql": {
                "host": row["Endpoint Address"],
                "port": row["Port"],
                "username": row["ReplicationUsername"],
                "password": row["ReplicationPassword"],
            }
        }
        dc["target_server_settings_cloud_sql"] = {
            "databaseVersion": row["Version"],
            "tier": "db-custom-1-3840",
            "dataDiskSizeGb": row["Storage"],
        }
        dc["location_dict"] = {"project_id": project_id, "region_id": region_id}
        dc["prefix_dict"] = {
            "prefix_cp_source": prefix_cp_source,
            "prefix_cp_cloudsql": prefix_cp_cloudsql,
            "prefix_mj": prefix_mj,
            "id": index,
        }
        dc["target_base_settings_cloud_sql"] = {
            "ipConfig": {"enableIpv4": True},
            "autoStorageIncrease": True,
            "dataDiskType": "PD_SSD",
            "rootPassword": "postgres",
        }
        list_migration_job.append(
            (
                dc["prefix_dict"],
                dc["location_dict"],
                dc["source_connection"],
                dc["target_base_settings_cloud_sql"],
                dc["target_server_settings_cloud_sql"],
            )
        )
    jobs = []
    for (a, b, c, d, e) in list_migration_job:
        p = multiprocessing.Process(target=func,args=(a,b,c,d,e,),)
        p.start()
        jobs.append(p)
#for (a,b,c,d,e) in list_migration_job:
#        #migration_job = dms.DataMigrationService(a, b, c, d, e)
#        #val = migration_job.get_state_from_migration_job()
"""
for mj in list_migration_job2:
    val = mj.get_state_from_migration_job()
    val_list.append(val)
print(pd.DataFrame(val_list))
"""
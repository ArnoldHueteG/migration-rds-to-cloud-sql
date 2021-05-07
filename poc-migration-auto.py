#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datamigration_wrapper as dms
from datetime import datetime
import pandas as pd
import multiprocessing
from multiprocessing import log_to_stderr, get_logger
import logging
import argparse

# In[2]:
parser = argparse.ArgumentParser()
parser.add_argument('--migration-file', type=str)
args = parser.parse_args()

migration_file = args.migration_file or 'Test GCP Migration.csv'

df_machine_types = pd.read_csv('machine_types.csv')
df_migration_data = pd.read_csv(migration_file)


# In[3]:


project_id = 'aws-rds-gcp-cloudsql'
region_id = 'us-east4'
prefix_cp_source = 'auto-cp-pg-'
prefix_cp_cloudsql = 'auto-cs-pg-'
prefix_mj='auto-mj-'
now_str = datetime.now().strftime("%Y%m%dt%H%M%S")


# In[4]:


list_migration_job = []
for index, row in df_migration_data.iterrows():
    dc={}
    dc["source_connection"] = {"postgresql": {"host": row["Endpoint Address"],
                                      "port": row["Port"],
                                      "username": row["ReplicationUsername"],
                                      "password": row["ReplicationPassword"]}}
    dc["target_server_settings_cloud_sql"] = { "databaseVersion": row["Version"],
                              "tier": "db-custom-1-3840",
                              "dataDiskSizeGb": row["Storage"]}
    dc["location_dict"] = {"project_id": project_id,
                  "region_id": region_id}
    dc["prefix_dict"] = {"prefix_cp_source": prefix_cp_source,
               "prefix_cp_cloudsql": prefix_cp_cloudsql,
               "prefix_mj": prefix_mj,
               "now_str": now_str,
                "id" : index}
    dc["target_base_settings_cloud_sql"] = { "ipConfig": {
                                "enableIpv4": True
                              },
                              "autoStorageIncrease": True,
                              "dataDiskType": "PD_SSD",
                              "rootPassword": "postgres"}
    list_migration_job.append((dc["prefix_dict"],dc["location_dict"],dc["source_connection"],dc["target_base_settings_cloud_sql"],dc["target_server_settings_cloud_sql"]))




# In[5]:


#list_migration_job


# In[10]:

#for migration_job in list_migration_job:
#    dms.generate_migration_job(*migration_job)
if __name__ == '__main__':
    #log_to_stderr()
    #logger = get_logger()
    #logger.setLevel(logging.INFO)
    
    jobs = []
    for (a,b,c,d,e) in list_migration_job:
        p = multiprocessing.Process(target=dms.generate_migration_job, args=(a,b,c,d,e,))
        p.start()
        jobs.append(p)
    for job in jobs:
        job.join()
        
import pickle
from os import listdir
from os.path import isfile, join
import pandas as pd
from dms_wrapper import DataMigrationService

files = listdir("data/output/pickles/")
print(files)

list_migration_job = []
for file in files:
    with open(f"data/output/pickles/{file}", "rb") as input:
        migration_job = pickle.load(input)
    list_migration_job.append(migration_job)

val_list = []
for mj in list_migration_job:
    val = mj.get_state_from_migration_job()
    val_list.append(val)
print(pd.DataFrame(val_list))

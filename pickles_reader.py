import pickle
from os import listdir
from os.path import isfile, join
import pandas as pd

files = listdir("pickles/")

list_migration_job = []
for file in files:
    with open(f"pickles/{file}", "rb") as input:
        migration_job = pickle.load(input)
    list_migration_job.append(migration_job)

val_list = []
for mj in list_migration_job:
    # print(mj.migration_job_id)
    val = mj.get_state_from_migration_job()
    val_list.append(val)
print(pd.DataFrame(val_list))

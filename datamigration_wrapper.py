from oauth2client.client import GoogleCredentials
from googleapiclient import discovery
from googleapiclient import errors
import json
import time
from datetime import datetime
import logging
from pgdb import connect

debug_mode = True

def setup_logger(name_logfile=None):
    
    logger = logging.getLogger(name_logfile)
    
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    
    if name_logfile is not None:
        fileHandler = logging.FileHandler(name_logfile, mode='w')
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
    
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    
    logger.setLevel(logging.DEBUG)
    return logger

def test_connection(source_connection,logger):
    source_connection = dict(source_connection["postgresql"])
    source_connection["database"]="postgres"
    try:
        conn = connect(database=source_connection['database'],
                             host=source_connection['host'],
                             user=source_connection['username'],
                             password=source_connection['password'],
                             port=source_connection['port'])
        conn.close()
        logger.debug("Your connection to {} was successful.".format(source_connection['host']))
        return True
    except Exception as e: 
        logger.debug("Your connection to {} was not successful.".format(source_connection['host']))
        logger.debug(e)
        return False
    
def create_connection_profile(project_id,region_id,request_dict,wait_to_be_ready=False,logger=None):
    #project_id,region_id = location_dict["project_id"],location_dict["region_id"]
    connection_profile_id = request_dict["displayName"]
    dms = discovery.build('datamigration','v1')
    response = request_dict
    if not debug_mode:
        response = dms.projects().locations().connectionProfiles().create(parent="projects/{}/locations/{}".format(project_id,region_id),
                                                                        connectionProfileId= connection_profile_id,
                                                                        body=request_dict).execute()
    logger.debug(json.dumps(response, indent=2))
    if wait_to_be_ready and not debug_mode:
        response={}
        while True:
            response = dms.projects().locations().connectionProfiles().get(name="projects/{}/locations/{}/connectionProfiles/{}".format(project_id,region_id,connection_profile_id)).execute()
            logger.debug(response.get("state"))
            if response.get("state")!="READY":
                time.sleep(20)
            else: 
                break
    
def create_migration_job(migration_job_id,project_id,region_id,request_dict,logger=None):
    dms = discovery.build('datamigration','v1')
    response = request_dict
    if not debug_mode:
        response = dms.projects().locations().migrationJobs().create(parent="projects/{}/locations/{}".format(project_id,region_id),
                                                                        migrationJobId=migration_job_id,
                                                                        body=request_dict).execute()
    logger.debug(json.dumps(response, indent=2))
    response={}
    if not debug_mode:
        while True:
            response = dms.projects().locations().migrationJobs().get(name="projects/{}/locations/{}/migrationJobs/{}".format(project_id,region_id,migration_job_id)).execute()
            logger.debug(response.get("state"))
            if response.get("state")!="NOT_STARTED":
                time.sleep(5)
            else: 
                break        
        response = dms.projects().locations().migrationJobs().start(name="projects/{}/locations/{}/migrationJobs/{}".format(project_id,region_id,migration_job_id)).execute()
        logger.debug(json.dumps(response, indent=2))
    response={}
    if not debug_mode:
        while True:
            response = dms.projects().locations().migrationJobs().get(name="projects/{}/locations/{}/migrationJobs/{}".format(project_id,region_id,migration_job_id)).execute()
            logger.debug(response.get("state"))
            if response.get("state")!="RUNNING":
                time.sleep(10)
            else: 
                break
            
def generate_migration_job(prefix_dict,
                           location_dict,
                           source_connection,
                           target_base_settings_cloud_sql,
                           target_server_settings_cloud_sql):
    #################################################################################
    prefix_cp_source,prefix_cp_cloudsql,prefix_mj,now_str = prefix_dict["prefix_cp_source"],prefix_dict["prefix_cp_cloudsql"],prefix_dict["prefix_mj"],prefix_dict["now_str"]
    id = prefix_dict["id"]
    project_id,region_id = location_dict["project_id"],location_dict["region_id"]
    rds_name = source_connection["postgresql"]["host"].split(".")[0]
    connection_profile_id_source = "{1}{0}-{2}-{3}".format(id,prefix_cp_source,rds_name,now_str)
    connection_profile_id_cloudsql = "{1}{0}-{2}-{3}".format(id,prefix_cp_cloudsql,rds_name,now_str)
    migration_job_id = "{1}{0}-{2}-{3}".format(id,prefix_mj,rds_name,now_str)
    logger = setup_logger('logs/{3}-{1}{0}-{2}.log'.format(id,prefix_mj,rds_name,now_str))
    logger.info('Initiate migration job')
    #################################################################################
    if not test_connection(source_connection,logger):
        logger.debug("migration job won't continue because connection test was not succesful")
        return
    request_dict =  {"displayName": connection_profile_id_source, **source_connection}
    #logger.debug(json.dumps(request_dict, indent=2))
    create_connection_profile(**location_dict,request_dict=request_dict,wait_to_be_ready=False,logger=logger)
    request_dict =  {"displayName": connection_profile_id_cloudsql,
                        "cloudsql": {
                          "settings": {
                                  **target_base_settings_cloud_sql,
                                  **target_server_settings_cloud_sql,
                                  "sourceId": "projects/{}/locations/{}/connectionProfiles/{}".format(project_id,region_id,connection_profile_id_source),
                                }
                        }
                    }
    #logger.debug(json.dumps(request_dict, indent=2))
    create_connection_profile(**location_dict,request_dict=request_dict,wait_to_be_ready=True,logger=logger)
    request_dict =  { "type" :"CONTINUOUS",
                     "source" : "projects/{}/locations/{}/connectionProfiles/{}".format(project_id,region_id,connection_profile_id_source),
                     "destination" : "projects/{}/locations/{}/connectionProfiles/{}".format(project_id,region_id,connection_profile_id_cloudsql),
                     "destinationDatabase" : {"provider": "RDS",
                                                  "engine": "POSTGRESQL"
                                                }
                    }
    #logger.debug(json.dumps(request_dict, indent=2))
    create_migration_job(migration_job_id=migration_job_id,**location_dict,request_dict=request_dict,logger=logger)
    
def get_state_from_migration_job(prefix_dict,
                           location_dict,
                           source_connection):
    #locals().update(prefix_dict) 
    prefix_cp_source,prefix_cp_cloudsql,prefix_mj,now_str = prefix_dict["prefix_cp_source"],prefix_dict["prefix_cp_cloudsql"],prefix_dict["prefix_mj"],prefix_dict["now_str"]
    id = prefix_dict["id"]
    project_id,region_id = location_dict["project_id"],location_dict["region_id"]
    rds_name = source_connection["postgresql"]["host"].split(".")[0]
    migration_job_id = "{1}{0}-{2}-{3}".format(id,prefix_mj,rds_name,now_str)
    dms = discovery.build('datamigration','v1')
    try:
        response = dms.projects().locations().migrationJobs().get(name="projects/{}/locations/{}/migrationJobs/{}".format(project_id,region_id,migration_job_id)).execute()
        state = response.get("state")
    except Exception as e:
        #print(e)
        state = e.resp.reason.upper()
    return state

def get_prefix(prefix_dict):
    locals().update(prefix_dict) 
    print(prefix_mj)
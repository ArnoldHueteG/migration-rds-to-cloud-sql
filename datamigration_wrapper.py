import json
import logging
import multiprocessing
import time
from datetime import datetime
import pickle

from googleapiclient import discovery, errors
from oauth2client.client import GoogleCredentials
from pgdb import connect

debug_mode = False


class DataMigrationService:
    def __init__(
        self,
        prefix_dict,
        location_dict,
        source_connection,
        target_base_settings_cloud_sql,
        target_server_settings_cloud_sql,
    ):
        self.prefix_dict = prefix_dict
        self.location_dict = location_dict
        self.source_connection = source_connection
        self.target_base_settings_cloud_sql = target_base_settings_cloud_sql
        self.target_server_settings_cloud_sql = target_server_settings_cloud_sql
        self.prefix_cp_source = prefix_dict["prefix_cp_source"]
        self.prefix_cp_cloudsql = prefix_dict["prefix_cp_cloudsql"]
        self.prefix_mj = prefix_dict["prefix_mj"]
        # self.now_str = prefix_dict["now_str"]
        self.now_str = datetime.now().strftime("%Y%m%dt%H%M%S")
        self.id = prefix_dict["id"]
        self.project_id = location_dict["project_id"]
        self.region_id = location_dict["region_id"]
        self.rds_name = source_connection["postgresql"]["host"].split(".")[0]
        self.connection_profile_id_source = "{1}{0}-{2}-{3}".format(
            self.id, self.prefix_cp_source, self.rds_name, self.now_str
        )
        self.connection_profile_id_cloudsql = "{1}{0}-{2}-{3}".format(
            self.id, self.prefix_cp_cloudsql, self.rds_name, self.now_str
        )
        self.migration_job_id = "{1}{0}-{2}-{3}".format(self.id, self.prefix_mj, self.rds_name, self.now_str)
        self.logfile_name = "logs/{3}-{1}{0}-{2}.log".format(self.id, self.prefix_mj, self.rds_name, self.now_str)
        # self.logfile_name = "logs/{1}{0}-{2}.log".format(self.id, self.prefix_mj, self.rds_name)
        self.picklefile_name = f"pickles/{self.id}-{self.rds_name}.pkl"
        self.logger = self.setup_logger(f"{self.id} - {self.rds_name}", self.logfile_name)
        self.logger.setLevel(logging.INFO)
        with open(self.picklefile_name, "wb") as output:
            pickle.dump(self, output)

    def setup_logger(self, logger_name=None, logfile_name=None):

        logger = logging.getLogger(logger_name)

        formatter = logging.Formatter(
            "%(asctime)s [ %(name)s ] %(levelname)s : %(message)s", datefmt="%Y/%m/%d %H:%M:%S"
        )

        if logfile_name is not None:
            fileHandler = logging.FileHandler(logfile_name, mode="w")
            fileHandler.setFormatter(formatter)
            logger.addHandler(fileHandler)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

        logger.setLevel(logging.DEBUG)
        return logger

    def generate_migration_job(self):
        self.logger.info("Starting migration job")
        if not self.test_connection():
            self.logger.info("migration job won't continue because connection test was not succesful")
            return
        request_dict = {"displayName": self.connection_profile_id_source, **self.source_connection}
        self.logger.info("Creating profile in dms service")
        self.create_connection_profile(request_dict=request_dict, wait_to_be_ready=False)
        request_dict = {
            "displayName": self.connection_profile_id_cloudsql,
            "cloudsql": {
                "settings": {
                    **target_base_settings_cloud_sql,
                    **target_server_settings_cloud_sql,
                    "sourceId": "projects/{}/locations/{}/connectionProfiles/{}".format(
                        self.project_id, self.region_id, self.connection_profile_id_source
                    ),
                }
            },
        }
        self.logger.info("Creating instance for replication")
        self.create_connection_profile(request_dict=request_dict, wait_to_be_ready=True)
        request_dict = {
            "type": "CONTINUOUS",
            "source": "projects/{}/locations/{}/connectionProfiles/{}".format(
                self.project_id, self.region_id, self.connection_profile_id_source
            ),
            "destination": "projects/{}/locations/{}/connectionProfiles/{}".format(
                self.project_id, self.region_id, self.connection_profile_id_cloudsql
            ),
            "destinationDatabase": {"provider": "RDS", "engine": "POSTGRESQL"},
        }
        self.logger.info("Creating migration job for replication")
        self.create_migration_job(request_dict=request_dict)

    def test_connection(self):
        source_connection = dict(self.source_connection["postgresql"])
        source_connection["database"] = "postgres"
        try:
            self.logger.info("Testing connection")
            conn = connect(
                database=source_connection["database"],
                host=source_connection["host"],
                user=source_connection["username"],
                password=source_connection["password"],
                port=source_connection["port"],
            )
            conn.close()
            self.logger.info("Your connection to {} was successful.".format(source_connection["host"]))
            return True
        except Exception as e:
            self.logger.info("Your connection to {} was not successful.".format(source_connection["host"]))
            self.logger.debug("{}".format(e).strip())
            return False

    def create_connection_profile(self, request_dict, wait_to_be_ready=False):
        connection_profile_id = request_dict["displayName"]
        dms = discovery.build("datamigration", "v1")
        response = request_dict
        if not debug_mode:
            response = (
                dms.projects()
                .locations()
                .connectionProfiles()
                .create(
                    parent="projects/{}/locations/{}".format(self.project_id, self.region_id),
                    connectionProfileId=connection_profile_id,
                    body=request_dict,
                )
                .execute()
            )
        self.logger.debug(json.dumps(response, indent=2))
        if wait_to_be_ready and not debug_mode:
            response = {}
            while True:
                response = (
                    dms.projects()
                    .locations()
                    .connectionProfiles()
                    .get(
                        name="projects/{}/locations/{}/connectionProfiles/{}".format(
                            self.project_id, self.region_id, connection_profile_id
                        )
                    )
                    .execute()
                )
                self.logger.debug(response.get("state"))
                if response.get("state") != "READY":
                    time.sleep(20)
                else:
                    break

    def create_migration_job(self, request_dict):
        dms = discovery.build("datamigration", "v1")
        dms.migration_jobs = dms.projects().locations().migrationJobs()
        response = request_dict
        if not debug_mode:
            response = dms.migration_jobs.create(
                parent="projects/{}/locations/{}".format(self.project_id, self.region_id),
                migrationJobId=self.migration_job_id,
                body=request_dict,
            ).execute()
        self.logger.debug(json.dumps(response, indent=2))
        response = {}
        if not debug_mode:
            while True:
                response = dms.migration_jobs.get(
                    name="projects/{}/locations/{}/migrationJobs/{}".format(
                        self.project_id, self.region_id, self.migration_job_id
                    )
                ).execute()
                self.logger.debug(response.get("state"))
                if response.get("state") != "NOT_STARTED":
                    time.sleep(5)
                else:
                    break
            response = dms.migration_jobs.start(
                name="projects/{}/locations/{}/migrationJobs/{}".format(
                    self.project_id, self.region_id, self.migration_job_id
                )
            ).execute()
            self.logger.debug(json.dumps(response, indent=2))
        response = {}
        if not debug_mode:
            while True:
                response = dms.migration_jobs.get(
                    name="projects/{}/locations/{}/migrationJobs/{}".format(
                        self.project_id, self.region_id, self.migration_job_id
                    )
                ).execute()
                self.logger.debug(response.get("state"))
                if response.get("state") != "RUNNING":
                    time.sleep(10)
                else:
                    break

    def get_state_from_migration_job(self):
        dms = discovery.build("datamigration", "v1")
        dms.migration_jobs = dms.projects().locations().migrationJobs()
        try:
            response = dms.migration_jobs.get(
                name="projects/{}/locations/{}/migrationJobs/{}".format(
                    self.project_id, self.region_id, self.migration_job_id
                )
            ).execute()
            state = response.get("state")
        except Exception as e:
            state = e.resp.reason.upper()
        response = {"host": self.rds_name, "migration_job_id": self.migration_job_id, "state": state}
        self.logger.debug(json.dumps(response, indent=2))
        return response


if __name__ == "__main__":
    prefix_dict = {
        "prefix_cp_source": "auto-cp-pg-",
        "prefix_cp_cloudsql": "auto-cs-pg-",
        "prefix_mj": "auto-mj-",
        # "now_str": "20210521t184653",
        "id": 0,
    }
    location_dict = {"project_id": "aws-rds-gcp-cloudsql", "region_id": "us-east4"}
    from local_secrets.secrets import *

    """
    source_connection = {
        "postgresql": {
            "host": "********************",
            "port": 5432,
            "username": "postgres",
            "password": "postgres",
        }
    }
    """
    target_base_settings_cloud_sql = {
        "ipConfig": {"enableIpv4": True},
        "autoStorageIncrease": True,
        "dataDiskType": "PD_SSD",
        "rootPassword": "postgres",
    }
    target_server_settings_cloud_sql = {
        "databaseVersion": "POSTGRES_12",
        "tier": "db-custom-1-3840",
        "dataDiskSizeGb": 15,
    }
    dms = DataMigrationService(
        prefix_dict,
        location_dict,
        source_connection,
        target_base_settings_cloud_sql,
        target_server_settings_cloud_sql,
    )
    dms.logger.setLevel(logging.DEBUG)
    dms.generate_migration_job()
    # dms.get_state_from_migration_job()

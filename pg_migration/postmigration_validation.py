#from pgdb import connect
import psycopg2
import itertools
import json
import os
import pickle
from queue import Queue
from threading import Thread
from timer import Timer
import logging
from local.secrets.environment_vars import BD_CONNECTION_1,BD_CONNECTION_2


class PostMigrationValidation:
    pass


logger = logging.getLogger()
formatter = logging.Formatter("%(asctime)s %(levelname)s : %(message)s", datefmt="%Y/%m/%d %H:%M:%S")
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.setLevel(logging.DEBUG)
t = Timer(logger)


class ListPostgresConn:
    def get_databases(self):
        logger.info("Getting databases")
        t.start()
        conn = self.connect_to_db("postgres")
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_catalog.pg_database")
        self.list_database = [
            _[0] for _ in cur.fetchall() if _[0] not in ["template1", "template0", "rdsadmin", "cloudsqladmin"]
        ]
        conn.close()
        cur.close()
        t.stop()

    def get_tables_metadata(self):
        logger.info("Getting list of schemas and tables")
        t.start()
        self.list_schema = []
        self.list_table = []
        for db in self.list_database:
            conn = self.connect_to_db(db)
            cur = conn.cursor()
            cur.execute("SELECT schema_name FROM information_schema.schemata")
            list_schema = [
                {"database": db, "schema": _[0]} for _ in cur.fetchall() if _[0] not in ["pg_temp_1", "pg_toast_temp_1"]
            ]
            self.list_schema.extend(list_schema)
            _ = ",".join([f"'{ _['schema'] }'" for _ in list_schema])
            cur.execute(f"SELECT schemaname,tablename FROM pg_catalog.pg_tables where schemaname in ({ _ })")
            list_table = [{"database": db, "schema": _[0], "table": _[1]} for _ in cur.fetchall()]
            self.list_table.extend(list_table)
            conn.close()
            cur.close()
        t.stop()

    def get_dumps_in_parallel(self):
        logger.info("Getting dumps")
        t.start()
        self.dump = {}
        q = Queue(maxsize=0)
        num_theads = min(40, len(self.list_table))
        results = [{} for x in self.list_table]
        for i in range(len(self.list_table)):
            q.put((i, self.list_table[i]))

        def crawl(q, result):
            logger.info(q.qsize())
            while not q.empty():
                work = q.get()
                dump = {}
                table = work[1]
                i = work[0]
                try:
                    dump_key = f'{table["database"]}.{table["schema"]}.{table["table"]}'
                    dump[dump_key] = os.popen(
                        f'pg_dump -h {self.host} -p {self.port} -U {self.user} -d {table["database"]} '
                        + f'--no-password --schema-only --table {table["schema"]}.{table["table"]}'
                    ).read()
                    result[i] = dump
                except:
                    result[i] = {}
                q.task_done()
            return True

        for i in range(num_theads):
            worker = Thread(target=crawl, args=(q, results))
            worker.setDaemon(True)
            worker.start()

        q.join()

        for d in results:
            self.dump.update(d)
        t.stop()

    def get_dumps(self):
        logger.info("Getting dumps")
        self.dump = {}
        t.start()
        for table in self.list_table:
            dump_key = f'{table["database"]}.{table["schema"]}.{table["table"]}'
            self.dump[dump_key] = os.popen(
                f'pg_dump -h {self.host} -p {self.port} -U {self.user} -d {table["database"]} '
                + f'--no-password --schema-only --table {table["schema"]}.{table["table"]}'
            ).read()
        t.stop()

    def __init__(self, host, port, user, password, parallel=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.get_databases()
        self.update_pgpass_conf()
        self.get_tables_metadata()
        if parallel:
            self.get_dumps_in_parallel()
        else:
            self.get_dumps()

    def connect_to_db(self, database):
        # conn = connect(database=database, host=self.host, user=self.user, password=self.password, port=self.port)
        conn = psycopg2.connect(dbname=database, host=self.host, user=self.user, password=self.password, port=self.port)
        return conn

    def update_pgpass_conf(self):
        logger.info("Setting pgpass.conf")
        t.start()
        with open(r"C:\Users\Arnold\AppData\Roaming\postgresql\pgpass.conf", "w+") as pgpass_conf:
            for database in self.list_database:
                pgpass_conf.write(f"{self.host}:{self.port}:{database}:{self.user}:{self.password}\n")
        t.stop()


if __name__ == "__main__":

    lpc1 = ListPostgresConn(host = BD_CONNECTION_1.split(":")[0], 
                            port = BD_CONNECTION_1.split(":")[1], 
                            user = BD_CONNECTION_1.split(":")[2], 
                            password = BD_CONNECTION_1.split(":")[3])

    lpc2 = ListPostgresConn(host = BD_CONNECTION_2.split(":")[0], 
                            port = BD_CONNECTION_2.split(":")[1], 
                            user = BD_CONNECTION_2.split(":")[2], 
                            password = BD_CONNECTION_2.split(":")[3])

    diff_database = list(set(lpc1.list_database) - set(lpc2.list_database))
    print(diff_database)

    lpc1.list_schema_same_db = [_ for _ in lpc1.list_schema if _["database"] not in (diff_database)]
    diff_schema = [x for x in lpc1.list_schema_same_db if x not in lpc2.list_schema]
    print(diff_schema)

    lpc1.list_table_same_db = [_ for _ in lpc1.list_table if _["database"] not in (diff_database)]
    diff_table = [x for x in lpc1.list_table_same_db if x not in lpc2.list_table]
    print(json.dumps(diff_table, indent=2))

    equal_table = [x for x in lpc1.list_table_same_db if x in lpc2.list_table]

    diff_dump = []
    for table in equal_table:
        dc_dump = {}
        dump_key = f'{table["database"]}.{table["schema"]}.{table["table"]}'
        func_remove_set = lambda x: "\n".join([line for line in x.splitlines() if not line.startswith("SET")])
        _0 = func_remove_set(lpc1.dump[dump_key])
        _1 = func_remove_set(lpc2.dump[dump_key])
        if not _0 == _1:
            dc_dump["table"] = dump_key
            dc_dump["select_for_compare"] = _0
            dc_dump["compare_to"] = _1
            diff_dump.append(dc_dump)
    with open("lpc1.pkl", "wb") as output:
        pickle.dump(lpc1, output)
    with open("lpc2.pkl", "wb") as output:
        pickle.dump(lpc2, output)

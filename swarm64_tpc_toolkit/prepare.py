
from multiprocessing.pool import Pool
from shlex import split
from subprocess import Popen


class PrepareBenchmarkFactory:
    TABLES = []

    def __init__(self, args):
        self.args = args
        self.pool = Pool(processes=10)

    @classmethod
    def _run_shell_task(cls, task):
        cmd = split(task)
        # p = Popen(cmd)
        # p.wait()
        print(cmd)

    def run(self):
        self.prepare_db()

        ingest_tasks = []
        for table in PrepareBenchmarkFactory.TABLES:
            ingest_tasks.extend(self.ingest(table))

        tasks = self.pool.map_async(PrepareBenchmarkFactory._run_shell_task, ingest_tasks)
        result = tasks.get()
        print(result)

        self.add_indexes()
        self.vacuum_analyze()

    def prepare_db(self):
        pass

    def ingest(self, table):
        return []

    def add_indexes(self):
# run_if_exists primary-keys.sql
# run_if_exists foreign-keys.sql
# run_if_exists indexes.sql
        pass

    def vacuum_analyze(self):
        PrepareBenchmarkFactory._run_shell_task(f'psql {self.args.dsn} -c "VACUUM"')

        analyze_tasks = [f'psql {self.args.dsn} -c "ANALYZE {table}"' for table in
                         PrepareBenchmarkFactory.TABLES]
        tasks = self.pool.map_async(PrepareBenchmarkFactory._run_shell_task, analyze_tasks)
        result = tasks.get()
        print(result)

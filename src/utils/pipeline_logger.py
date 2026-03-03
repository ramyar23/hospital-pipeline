# src/utils/pipeline_logger.py
from datetime import datetime
from src.utils.helper_functions import print_header


class PipelineLogger:
    def __init__(self, spark, project_name, log_table):
        self.spark        = spark
        self.project      = project_name
        self.log_table    = log_table
        self.run_id       = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.start_time   = datetime.now()
        self.layer_times  = {}
        self.layer_counts = {}
        print_header(f'{self.project} STARTED')
        print(f'  Run ID: {self.run_id}')

    def start_layer(self, name):
        self.layer_times[name] = datetime.now()
        print(f'\n  [{name.upper()}] Starting...')

    def end_layer(self, name, df):
        secs  = (datetime.now() -
                 self.layer_times[name]).seconds
        count = df.count()
        self.layer_counts[name] = count
        print(f'  [{name.upper()}] Done! '
              f'Rows: {count:,} | Time: {secs}s')

    def finish(self, status='success', error=None):
        total  = (datetime.now() - self.start_time).seconds
        bronze = self.layer_counts.get('bronze', 0)
        silver = self.layer_counts.get('silver', 0)
        gold   = self.layer_counts.get('gold',   0)

        log_df = self.spark.createDataFrame([(
            self.run_id, self.project,
            str(self.start_time), str(datetime.now()),
            total, bronze, silver, gold,
            bronze - silver, status,
            str(error) if error else 'None'
        )], ['run_id','project','started','ended','secs',
             'bronze','silver','gold','dropped',
             'status','error'])

        log_df.write.mode('append') \
               .option('overwriteSchema','true') \
               .saveAsTable(self.log_table)

        print_header(f'{self.project} COMPLETED')
        print(f'  Status : {status}')
        print(f'  Time   : {total}s')
        print(f'  Bronze : {bronze:,} rows')
        print(f'  Silver : {silver:,} rows')
        print(f'  Dropped: {bronze - silver:,} rows')
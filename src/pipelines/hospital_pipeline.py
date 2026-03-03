# src/pipelines/hospital_pipeline.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.bronze_layer    import BronzeLayer
from src.utils.silver_layer    import SilverLayer
from src.utils.quality_checker import QualityChecker
from src.utils.pipeline_logger import PipelineLogger
from src.utils.helper_functions import save_table


class HospitalPipeline:

    # ══ CHANGE ONLY THIS SECTION PER PROJECT ══
    PROJECT_NAME  = 'Hospital_Pipeline'
    BRONZE_DB     = 'hospital_bronze'
    SILVER_DB     = 'hospital_silver'
    GOLD_DB       = 'hospital_gold'
    LOG_TABLE     = 'hospital_gold.pipeline_logs'
    RAW_TABLE     = 'hospital_bronze.raw_appointments'
    SOURCE_NAME   = 'hospital_app'
    PRIMARY_KEY   = 'appointment_id'
    AMOUNT_COL    = 'bill_amount'
    TS_COL        = 'visit_date'
    DOUBLE_COLS   = ['bill_amount', 'rating']
    INT_COLS      = ['age', 'wait_time_mins']
    TS_COLS       = ['visit_date']
    NOT_NULL_COLS = ['appointment_id', 'doctor_id']
    POSITIVE_COLS = ['bill_amount']
    FILL_DICT     = {'rating': 4.0, 'tip_amount': 0.0}
    INITCAP_COLS  = ['department', 'city']
    LOWER_COLS    = ['status', 'gender']
    VALID_STATUS  = ['completed', 'cancelled', 'no_show']
    ENTITY_COL    = 'doctor_id'
    CATEGORY_COL  = 'department'
    AGE_COL       = 'age'

    def __init__(self, spark):
        self.spark  = spark
        self.logger = PipelineLogger(
            spark, self.PROJECT_NAME, self.LOG_TABLE)
        self.bronze = BronzeLayer(spark, self.BRONZE_DB)
        self.silver = SilverLayer(spark, self.SILVER_DB)

    def run(self):
        try:
            self._bronze()
            self._silver()
            self._gold()
            self.logger.finish(status='success')
        except Exception as e:
            self.logger.finish(status='failed',
                               error=str(e))
            raise

    def _bronze(self):
        self.logger.start_layer('bronze')
        raw_df   = self.bronze.read_table(self.RAW_TABLE)
        self.bdf = self.bronze.run(
            raw_df, self.SOURCE_NAME, 'trips')
        QualityChecker(self.bdf, 'bronze') \
            .check_not_empty() \
            .check_no_nulls(self.PRIMARY_KEY) \
            .result()
        self.logger.end_layer('bronze', self.bdf)

    def _silver(self):
        self.logger.start_layer('silver')
        df = self.silver.fix_types(
            self.bdf,
            double_cols = self.DOUBLE_COLS,
            int_cols    = self.INT_COLS,
            ts_cols     = self.TS_COLS)
        df = self.silver.remove_invalid(
            df,
            not_null_cols = self.NOT_NULL_COLS,
            positive_cols = self.POSITIVE_COLS)
        df = self.silver.fill_nulls(df, self.FILL_DICT)
        df = self.silver.remove_duplicates(
            df, self.PRIMARY_KEY)
        df = self.silver.standardize(
            df,
            initcap_cols = self.INITCAP_COLS,
            lower_cols   = self.LOWER_COLS)
        df = self.silver.add_derived(
            df, self.TS_COL, self.AGE_COL)
        self.silver.save(df, 'trips')
        QualityChecker(df, 'silver') \
            .check_not_empty() \
            .check_no_nulls(self.PRIMARY_KEY) \
            .check_positive(self.AMOUNT_COL) \
            .check_no_duplicates(self.PRIMARY_KEY) \
            .check_valid_values(
                'status', self.VALID_STATUS) \
            .result()
        self.logger.end_layer('silver', df)
        self.sdf = df

    def _gold(self):
        self.logger.start_layer('gold')
        completed = self.sdf.filter(
            F.col('status') == 'completed')

        # Gold Table 1: Doctor KPIs
        entity = completed \
            .groupBy(self.ENTITY_COL) \
            .agg(
                F.count(self.PRIMARY_KEY)
                 .alias('total_records'),
                F.round(F.sum(self.AMOUNT_COL), 2)
                 .alias('total_revenue'),
                F.round(F.avg(self.AMOUNT_COL), 2)
                 .alias('avg_amount'),
                F.round(F.avg('rating'), 2)
                 .alias('avg_rating')
            ) \
            .withColumn('rank',
                F.rank().over(Window.orderBy(
                    F.desc('total_revenue')))) \
            .withColumn('tier',
                F.when(F.col('rank') <= 3,  'Gold')
                 .when(F.col('rank') <= 10, 'Silver')
                 .otherwise('Bronze'))
        save_table(entity,
            f'{self.GOLD_DB}.entity_kpis')

        # Gold Table 2: Department KPIs
        category = completed \
            .groupBy(self.CATEGORY_COL) \
            .agg(
                F.count(self.PRIMARY_KEY)
                 .alias('total_records'),
                F.round(F.sum(self.AMOUNT_COL), 0)
                 .alias('total_revenue'),
                F.round(F.avg(self.AMOUNT_COL), 2)
                 .alias('avg_amount')
            ) \
            .orderBy(F.desc('total_revenue'))
        save_table(category,
            f'{self.GOLD_DB}.category_kpis')

        # Gold Table 3: Daily Trends
        date_w = Window.orderBy('event_date')
        run_w  = date_w.rowsBetween(
            Window.unboundedPreceding, 0)
        daily = completed \
            .groupBy('event_date') \
            .agg(
                F.count(self.PRIMARY_KEY)
                 .alias('daily_records'),
                F.round(F.sum(self.AMOUNT_COL), 2)
                 .alias('daily_revenue')
            ) \
            .orderBy('event_date'
                     ) \
            .withColumn('running_total',
                F.round(F.sum(
                    'daily_revenue').over(run_w), 2)) \
            .withColumn('prev',
                F.lag('daily_revenue', 1)
                 .over(date_w)) \
            .withColumn('trend',
                F.when(F.col('daily_revenue') >
                       F.col('prev'), 'Up')
                 .when(F.col('daily_revenue')
                       F.col('prev'), 'Down')
                 .otherwise('Same'))
        save_table(daily,
            f'{self.GOLD_DB}.daily_trends')
        self.logger.end_layer('gold', daily)
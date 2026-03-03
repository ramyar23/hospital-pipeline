# src/utils/silver_layer.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.helper_functions import (
    add_time_columns, save_table)


class SilverLayer:
    def __init__(self, spark, database):
        self.spark    = spark
        self.database = database
        print(f'SilverLayer ready: {database}')

    def fix_types(self, df, double_cols=[],
                  int_cols=[], ts_cols=[]):
        for c in double_cols:
            df = df.withColumn(c, F.col(c).cast('double'))
        for c in int_cols:
            df = df.withColumn(c, F.col(c).cast('integer'))
        for c in ts_cols:
            df = df.withColumn(c,
                F.to_timestamp(c, 'yyyy-MM-dd HH:mm:ss'))
        print('  Step 1: Types fixed')
        return df

    def remove_invalid(self, df, not_null_cols=[],
                       positive_cols=[]):
        before = df.count()
        for c in not_null_cols:
            df = df.filter(F.col(c).isNotNull())
        for c in positive_cols:
            df = df.filter(F.col(c) > 0)
        print(f'  Step 2: Removed '
              f'{before - df.count():,} invalid rows')
        return df

    def fill_nulls(self, df, fill_dict):
        print('  Step 3: Nulls filled')
        return df.fillna(fill_dict)

    def remove_duplicates(self, df, pk):
        before = df.count()
        w  = Window.partitionBy(pk) \
                   .orderBy(F.desc('_loaded_at'))
        df = df.withColumn('_rn', F.row_number().over(w)) \
               .filter(F.col('_rn') == 1).drop('_rn')
        print(f'  Step 4: Removed '
              f'{before - df.count():,} duplicates')
        return df

    def standardize(self, df, initcap_cols=[],
                    lower_cols=[]):
        for c in initcap_cols:
            df = df.withColumn(c,
                F.initcap(F.trim(F.col(c))))
        for c in lower_cols:
            df = df.withColumn(c,
                F.lower(F.trim(F.col(c))))
        print('  Step 5: Text standardized')
        return df

    def add_derived(self, df, ts_col, age_col=None):
        df = add_time_columns(df, ts_col)
        if age_col:
            df = df.withColumn('age_group',
                F.when(F.col(age_col) < 18, 'Child')
                 .when(F.col(age_col) < 60, 'Adult')
                 .otherwise('Senior'))
        print('  Step 6: Derived columns added')
        return df

    def save(self, df, table_name):
        save_table(df,
            f'{self.database}.{table_name}')
        return df
```

**Press Ctrl+S to save!**

---

**Your utils folder should now have ALL 5 files:**
```

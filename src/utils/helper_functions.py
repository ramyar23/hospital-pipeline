# src/utils/helper_functions.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


def print_header(title, width=55):
    print('\n' + '='*width)
    print(f'  {title}')
    print('='*width)


def save_table(df, table_name, mode='overwrite'):
    df.write \
      .mode(mode) \
      .option('overwriteSchema', 'true') \
      .saveAsTable(table_name)
    count = df.count()
    print(f'  Saved {table_name}: {count:,} rows')
    return count


def add_metadata(df, source_name):
    return df \
        .withColumn('_loaded_at', F.current_timestamp()) \
        .withColumn('_source',    F.lit(source_name)) \
        .withColumn('_layer',     F.lit('bronze')) \
        .withColumn('_batch_id',  F.lit(
            datetime.now().strftime('%Y%m%d_%H%M%S')))


def add_time_columns(df, ts_col):
    return df \
        .withColumn('event_hour',  F.hour(ts_col)) \
        .withColumn('event_month', F.month(ts_col)) \
        .withColumn('event_date',  F.to_date(ts_col)) \
        .withColumn('is_weekend',
            F.dayofweek(ts_col).isin([1,7]).cast('boolean')) \
        .withColumn('time_of_day',
            F.when(F.hour(ts_col).between(6,11),  'Morning')
             .when(F.hour(ts_col).between(12,16), 'Afternoon')
             .when(F.hour(ts_col).between(17,20), 'Evening')
             .otherwise('Night'))
```

---

**After pasting — press Ctrl+S to save!**
```
You should see the file name in the
left panel:
utils/
    __init__.py
    helper_functions.py   ← this one!
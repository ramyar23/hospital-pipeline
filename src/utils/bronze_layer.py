# src/utils/bronze_layer.py
from src.utils.helper_functions import add_metadata, save_table


class BronzeLayer:
    def __init__(self, spark, database):
        self.spark    = spark
        self.database = database
        print(f'BronzeLayer ready: {database}')

    def read_csv(self, path):
        df = self.spark.read \
                 .option('header', 'true') \
                 .option('inferSchema', 'true') \
                 .csv(path)
        print(f'  Read CSV: {df.count():,} rows')
        return df

    def read_table(self, table_name):
        df = self.spark.table(table_name)
        print(f'  Read table: {df.count():,} rows')
        return df

    def run(self, df, source_name, table_name):
        bronze_df = add_metadata(df, source_name)
        save_table(bronze_df,
                   f'{self.database}.{table_name}')
        return bronze_df
```

**Press Ctrl+S to save!**

---

**Your utils folder should now have 4 files:**
```

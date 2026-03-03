# src/utils/quality_checker.py
from pyspark.sql import functions as F


class QualityChecker:
    def __init__(self, df, table_name):
        self.df         = df
        self.table_name = table_name
        self.passed     = 0
        self.failed     = 0
        print(f'  Checking: {table_name}')

    def check_not_empty(self):
        count = self.df.count()
        if count > 0:
            print(f'  PASS: {count:,} rows')
            self.passed += 1
        else:
            print(f'  FAIL: Table is EMPTY!')
            self.failed += 1
        return self

    def check_no_nulls(self, column):
        n = self.df.filter(F.col(column).isNull()).count()
        if n == 0:
            print(f'  PASS: No nulls in {column}')
            self.passed += 1
        else:
            print(f'  FAIL: {n} nulls in {column}!')
            self.failed += 1
        return self

    def check_positive(self, column):
        bad = self.df.filter(F.col(column) <= 0).count()
        if bad == 0:
            print(f'  PASS: {column} all positive')
            self.passed += 1
        else:
            print(f'  FAIL: {bad} rows with {column} <= 0!')
            self.failed += 1
        return self

    def check_no_duplicates(self, pk):
        dups = self.df.count() - \
               self.df.dropDuplicates([pk]).count()
        if dups == 0:
            print(f'  PASS: No duplicates in {pk}')
            self.passed += 1
        else:
            print(f'  FAIL: {dups} duplicates in {pk}!')
            self.failed += 1
        return self

    def check_valid_values(self, column, valid_list):
        bad = self.df.filter(
            ~F.col(column).isin(valid_list)).count()
        if bad == 0:
            print(f'  PASS: {column} all valid')
            self.passed += 1
        else:
            print(f'  FAIL: {bad} invalid in {column}!')
            self.failed += 1
        return self

    def result(self):
        print(f'  Result: {self.passed} passed, '
              f'{self.failed} failed')
        if self.failed > 0:
            raise Exception(
                f'Quality FAILED: {self.table_name}!')
        print('  ALL CHECKS PASSED!')
        return True
```

**Press Ctrl+S to save!**

---

**Your utils folder should now have:**
```
utils/
    __init__.py
    helper_functions.py  ✅
    quality_checker.py   ← just created!
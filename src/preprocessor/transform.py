from typing import Any
from pyspark.ml import Transformer
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame


class DatasetFingerprintCheck:
    """
    Check's to see that the data indeed is the forecasting data.
    """

    @staticmethod
    def check_data_context(df: SparkDataFrame):
        pass


class SplitDateToComponents(Transformer):
    def __init__(self) -> None:
        super().__init__()

    def _transform(self, dataset: SparkDataFrame) -> SparkDataFrame:
        return (
            dataset.withColumn("Year", F.year(F.col("Date")))
            .withColumn("Month", F.month(F.col("Date")))
            .withColumn("Day"),
            F.dayofmonth(F.col("Date")),
        )


class RemoveClosedStoreDays(Transformer):
    def __init__(self) -> None:
        super().__init__()

    def _transform(self, dataset: SparkDataFrame) -> SparkDataFrame:
        return dataset.filter(F.col("Open") == 1)


# TODO: Review to see if this belongs here.
class MergeStoreData(Transformer):
    def __init__(self, store_raw: SparkDataFrame) -> None:
        self.store_raw = store_raw
        super().__init__()

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.join(self.store_raw, on="Store")


class CompetitionOpenForMonths(Transformer):
    def __init__(self) -> None:
        super().__init__()

    def _transform(self, dataset: SparkDataFrame) -> SparkDataFrame:
        return dataset.withColumn(
            "CompetitionOpenForMonths",
            F.months_between(
                F.col("Date"),
                F.make_date(
                    F.col("CompetitionOpenSinceYear"),
                    F.col("CompetitionOpenSinceMonth"),
                    F.lit(1),
                ),
            ),
        )

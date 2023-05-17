from pyspark.sql.dataframe import DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.ml import Transformer


class MyTransformer(Transformer):
    def __init__(self) -> None:
        super().__init__()

    def _transform(self, dataset: DataFrame) -> DataFrame:

        raise NotImplementedError()


class PySparkPreprocessor:
    def __init__(self) -> None:
        pass

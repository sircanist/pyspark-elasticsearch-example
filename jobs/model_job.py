"""
Description
"""

from mleap import pyspark

from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.sql import DataFrame
from pyspark.sql.functions import window, date_format

from dependencies import constants
from dependencies.udf_custom import get_udf
from jobs.AbstractJob import AbstractJob


class ModelJob(AbstractJob):

    def learn_model(self):

        def build_features_vector(df, featuresCol='features'):
            """Build the feature vector. To simplify, we only consider the numeric features. For a more accurate model, we should
            encode the categorical features and use them as well."""
            inputCols = ["event_id", "id"]
            assembler = VectorAssembler(inputCols=inputCols, outputCol=featuresCol)
            return assembler.transform(df)


        df = self.es_reader.load("logs-endpoint-winevent-sysmon-*/")
        df = df.select("event_id", "winlog.process.thread.id")

        df = build_features_vector(df)
        df.select("event_id", "id", "features").show()

        return model


    def transform_input(self, df):

        config = self.config
        watermark = config["watermark"]
        time_window = config["time_window"]
        group_by = config["group_by"]
        weektime_format = config["weektime_format"]


def main():
    job = ModelJob(app_name="simple_job",
                   files="configs/etl_config.json",
                   jar_packages=
                   [
                       "org.elasticsearch:elasticsearch-hadoop:7.7.0",
                       "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
                   ]
                   )
    job.start()


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

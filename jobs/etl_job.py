"""
Description
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import window, date_format

from dependencies import constants
from dependencies.udf_custom import get_udf
from jobs.AbstractJob import AbstractJob


class SimpleJob(AbstractJob):

    def learn_model(self):
        return None

    def transform_input(self, df):
        config = self.config
        watermark = config["watermark"]

        group_by = config["group_by"]
        time_window = config["time_window"]
        weektime_format = config["weektime_format"]

        def foreach_adapter(df: DataFrame, _):
            print("foreach adapter started")
            if not df.rdd.isEmpty():
                anomaly_cum_average = get_udf("anomaly_cum_average", self.config)
                df_anomaly = df.withColumn("is_anomaly",
                                           anomaly_cum_average("count", "weektime", *self.group_by))
                df_result = df_anomaly.filter("is_anomaly == False")
                self.write_to_es(df_result)

                print("finished writing")

        grouped_df = df.withWatermark("timestamp", watermark) \
            .groupBy(*group_by, window("timestamp", time_window)) \
            .count().withColumn("weektime", date_format('window.end', weektime_format)) \
            .withColumn("@timestamp", date_format('window.end', constants.TIMESTAMP_FORMAT))

        return grouped_df.writeStream \
            .foreachBatch(foreach_adapter).start()


def main():
    job = SimpleJob(app_name="simple_job",
                    files="configs/etl_config.json",
                    jar_packages=
                    [
                        "org.elasticsearch:elasticsearch-hadoop:7.6.2",
                        "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
                    ]
                    )
    job.start()


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

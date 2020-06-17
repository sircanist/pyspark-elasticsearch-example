from abc import ABC, abstractmethod

from pyspark.sql.functions import schema_of_json, date_format

from dependencies import constants
from dependencies.spark import start_spark
from pyspark.sql.functions import from_json, to_timestamp
from pyspark.sql import functions as F
import json


class AbstractJob(ABC):

    TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"

    def __init__(self, app_name, files, jar_packages, dependencies = "packages.zip"):
        # start Spark application and get Spark session, logger and config
        self.app_name = app_name
        spark, log, config = start_spark(
            app_name=app_name,
            files=[files],
            master="192.168.122.3:7077",
            jar_packages=jar_packages,
            dependencies=dependencies
        )
        self.kafka_server = config["kafka_server"]
        self.es_server = config["es_server"]
        self.log = log
        self.spark = spark
        self.config = config
        self.es_reader = (spark.read
                     .format("org.elasticsearch.spark.sql")
                     .option("es.nodes", self.es_server)
                     .option("es.net.http.auth.user","elastic")
                     .option("es.read.field.as.array.include", "winlog.keywords,etl_pipeline")
                     .option("es.read.field.exclude", "tags,user,z_original_message,z_logstash_pipeline")
                     )

    def start(self):
        self.model = self.learn_model()
        input = self.read_input()
        query = self.transform_input(input)
        query.awaitTermination()

    def read_input(self):
        config = self.config
        spark = self.spark

        kafka_server = config["kafka_server"]
        kafka_topic = config["kafka_topic"]
        schema_input = config["schema_input"]

        startInput = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as value")
          )

        inputAnomalySchema = schema_of_json(F.lit(json.dumps(schema_input)))

        return (
            startInput.withColumn("data", from_json("value", inputAnomalySchema))
            .select('data.*')
            .withColumn("timestamp", to_timestamp("@timestamp"))
            .withColumn(
                "@timestamp", date_format('timestamp', constants.TIMESTAMP_FORMAT)
            )
        )

    @abstractmethod
    def transform_input(self, input):
        pass

    @abstractmethod
    def learn_model(self):
        pass

    def write_to_es(self, df):
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", self.es_server) \
            .option("es.resource", f"es_spark/{self.app_name}") \
            .save(mode="append")

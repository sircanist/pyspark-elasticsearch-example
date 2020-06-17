"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""
from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}, dependencies = None):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # append default file
    files.insert(0, "configs/default_config.json")

    # append packages zip file with all dependencies
    if dependencies:
        files.append(dependencies)

    # detect execution environment
    flag_debug = 'DEBUG' in environ.keys()

    config_dict = {}
    if flag_debug or master is None:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .enableHiveSupport()
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master("spark://{}".format(master))
            .enableHiveSupport()
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        # add packages.zip as a dependency
        spark_builder.config('spark.submit.pyFiles', dependencies)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    spark_logger = logging.Log4j(spark_sess)
    if config_files:
        for config_file in config_files:
            path_to_config_file = path.join(spark_files_dir, config_file)
            with open(path_to_config_file, 'r') as config_file:
                config_dict.update(json.load(config_file))
            spark_logger.info('loaded config from ' + config_file.name)
    else:
        spark_logger.warning('no config file found')
        config_dict = None



    return spark_sess, spark_logger, config_dict

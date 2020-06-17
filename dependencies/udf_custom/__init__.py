from functools import partial

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from dependencies.udf_custom.CumAverage import _anomaly_cum_average

udf_mapper = {
    "anomaly_cum_average": _anomaly_cum_average,
}


def get_udf(name, config):
    udf_function = udf_mapper.get(name)
    return udf(lambda *args: udf_function(*args, config=config) )

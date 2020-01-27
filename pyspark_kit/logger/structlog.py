import rapidjson
import structlog
from pyspark.sql import SparkSession

from pyspark_kit.logger.log4j_proxy import Log4j


def new_logger(spark: SparkSession):
    return structlog.wrap_logger(
        Log4j(spark),
        processors=[
            structlog.stdlib.filter_by_level,
            # structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(serializer=rapidjson.dumps)
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

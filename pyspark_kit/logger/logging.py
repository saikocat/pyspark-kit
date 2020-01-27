# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)
# LOGGER.info("pyspark script logger initialized")


class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def trace(self, message):
        self.logger.trace(message)
        return None

    def debug(self, message):
        self.logger.debug(message)
        return None

    def info(self, message):
        self.logger.info(message)
        return None

    def warn(self, message):
        self.logger.warn(message)
        return None

    def error(self, message):
        self.logger.error(message)
        return None

    def fatal(self, message):
        self.logger.fatal(message)
        return None

    def set_level(self, level):
        self.logger.setLevel(level)
        return None

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local")
        .appName("local_pyspark_pytest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.default.parallelism", 2)
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def spark_context(spark):
    return spark.sparkContext


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """ Disable stopping the shared spark session during tests """

    def noop(*args, **kwargs):
        print("Disabled spark.stop for testing")

    monkeypatch.setattr("pyspark.sql.SparkSession.stop", noop)

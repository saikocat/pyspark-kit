from typing import Union

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class AvroSchemaConverter:
    """Convert to and from Avro Schema.

    Requires: --packages org.apache.spark:spark-avro_{scalaVersion}:2.4.3
    """

    def __init__(self, spark: SparkSession):
        """TODO: find a way to pass existing SparkConf instead"""
        from py4j.java_gateway import java_import

        java_import(spark._jvm, "org.apache.spark.sql.avro.SchemaConverters")
        self.spark = spark
        self.structype_from_json = (
            spark._jvm.org.apache.spark.sql.types.StructType.fromJson
        )
        self.schema_converters = spark._jvm.org.apache.spark.sql.avro.SchemaConverters

    def from_spark_schema(
        self,
        catalyst_type: Union[StructType, str],
        nullable: bool = False,
        record_name: str = "topLevelRecord",
        namespace: str = "",
    ) -> str:
        def catalyst_type_to_json(catalyst_type: Union[StructType, str]) -> str:
            if isinstance(catalyst_type, StructType):
                return str(catalyst_type.json())
            return str(catalyst_type)

        spark_schema_json = catalyst_type_to_json(catalyst_type)
        avsc = self.schema_converters.toAvroType(
            self.structype_from_json(spark_schema_json), nullable, record_name, namespace
        )
        return str(avsc.toString())

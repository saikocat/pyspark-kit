from typing import Union

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StructType, _parse_datatype_json_string


class AvroSchemaConverter:
    """Convert to and from Avro Schema.

    Requires: --packages org.apache.spark:spark-avro_{scalaVersion}:2.4.3
    """

    def __init__(self, spark: SparkSession):
        """TODO: find a way to pass existing SparkConf instead"""
        java_import(spark._jvm, "org.apache.spark.sql.avro.SchemaConverters")
        java_import(spark._jvm, "org.apache.avro.Schema")
        self.spark = spark
        self.structype_from_json = (
            spark._jvm.org.apache.spark.sql.types.StructType.fromJson
        )
        self.schema_converters = spark._jvm.org.apache.spark.sql.avro.SchemaConverters

    def to_spark_schema(self, avsc_json: str) -> DataType:
        avsc_jvm = self.spark._jvm.org.apache.avro.Schema.Parser().parse(avsc_json)
        spark_type_jvm = self.schema_converters.toSqlType(avsc_jvm)
        return _parse_datatype_json_string(spark_type_jvm.dataType().json())

    def to_json_from_spark_schema(
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
        avsc_jvm = self.schema_converters.toAvroType(
            self.structype_from_json(spark_schema_json),
            nullable,
            record_name,
            namespace,
        )
        return str(avsc_jvm.toString())

from datetime import date

from pyspark_kit.sdc.id_generator import generate_sequential_ids, Hint

from pyspark_kit.pytest_helper.spark_fixtures import spark, df_equals, row_to_dict
from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    StructField,
)


def test_generate_sequential_ids(spark, df_equals):
    data = [
        (1, 123, "Amazon", 10.5, True, date.today()),
        (2, 234, "Lazada", 15.5, False, date.today()),
    ]

    # For testing offset data
    data_10_offset = [
        (11, 123, "Amazon", 10.5, True, date.today()),
        (12, 234, "Lazada", 15.5, False, date.today()),
    ]

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("merchant_code", LongType(), True),
            StructField("name", StringType(), True),
            StructField("commission_rate", DoubleType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_date", DateType(), True),
        ]
    )

    expected_df = spark.createDataFrame(data, schema)
    # Simulate distributed env with into 2 partitions
    input_df = expected_df.repartition(2).drop("id")

    test_cases = [
        {
            "name": "Generate ids successfully with default params",
            "params": {},
            "expected": expected_df,
        },
        {
            "name": "Generate ids successfully with SINGLE_PARTITION hint",
            "params": {"hint": Hint.SINGLE_PARTITION},
            "expected": expected_df,
        },
        {
            "name": "Generate ids successfully with SINGLE_PARTITION hint + sortable_col",
            "params": {"sortable_col": "merchant_code", "hint": Hint.SINGLE_PARTITION},
            "expected": expected_df,
        },
        {
            "name": "Generate ids successfully with DISTRIBUTED hint",
            "params": {"hint": Hint.DISTRIBUTED},
            "expected": expected_df,
        },
        {
            "name": "Generate ids successfully with offset set",
            "params": {"offset": 10},
            "expected": spark.createDataFrame(data_10_offset, schema)
        },
    ]

    for test in test_cases:
        actual_df = generate_sequential_ids(input_df, **test["params"])
        assert df_equals(test["expected"], actual_df), test["name"]
        # we also assert the schema type are preserved
        assert test["expected"].schema == actual_df.schema

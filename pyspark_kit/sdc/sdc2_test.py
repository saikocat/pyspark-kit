from datetime import date, timedelta

from pyspark_kit.sdc import generate_sequential_ids, Hint
from pyspark_kit.sdc.sdc2 import ScdProcessor

from pyspark_kit.pytest_helper.spark_fixtures import spark, df_equals, row_to_dict
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    StructField,
)


def test_scd_processor(spark, df_equals):
    today = date.today()
    default_valid_from_date = today - timedelta(days=5)
    default_valid_end_date = date(year=9999, month=12, day=31)

    existing_schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("merchant_code", StringType(), True),
            StructField("name", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("est_year", LongType(), True),
            StructField("scd_valid_start_date", DateType(), True),
            StructField("scd_valid_end_date", DateType(), True),
            StructField("scd_is_active", BooleanType(), True),
            StructField("scd_version", LongType(), True),
        ]
    )

    source_schema = StructType(
        [
            StructField("merchant_code", StringType(), True),
            StructField("name", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("est_year", LongType(), True),
        ]
    )

    existing_data = [
        (
            1,
            "AMZ",
            "Amazon",
            10.0,
            2000,
            default_valid_from_date,
            default_valid_end_date,
            True,
            1,
        ),
        (
            2,
            "LAZ",
            "Lazada",
            10.0,
            2015,
            default_valid_from_date - timedelta(days=1),
            default_valid_from_date,
            False,
            1,
        ),
        (
            3,
            "LAZ",
            "Lazada",
            15.0,
            2015,
            default_valid_from_date,
            default_valid_end_date,
            True,
            2,
        ),
        (
            4,
            "PHV",
            "PhongVu",
            10.0,
            2015,
            default_valid_from_date,
            default_valid_end_date,
            True,
            1,
        ),
    ]

    source_data = [
        ("SHP", "Shopee", 10.0, 2015),  # new record
        ("LAZ", "Lazada", 20.0, 2015),  # attributes changed
        ("PHV", "PhongVu", 10.0, 2015),  # attributes unchanged
    ]

    expected_data = [
        (
            1,
            "AMZ",
            "Amazon",
            10.0,
            2000,
            default_valid_from_date,
            default_valid_end_date,
            True,
            1,
        ),
        (
            2,
            "LAZ",
            "Lazada",
            10.0,
            2015,
            default_valid_from_date - timedelta(days=1),
            default_valid_from_date,
            False,
            1,
        ),
        (
            4,
            "PHV",
            "PhongVu",
            10.0,
            2015,
            default_valid_from_date,
            default_valid_end_date,
            True,
            1,
        ),
        (
            3,
            "LAZ",
            "Lazada",
            15.0,
            2015,
            default_valid_from_date,
            today - timedelta(days=1),
            False,
            2,
        ),
        (5, "SHP", "Shopee", 10.0, 2015, today, default_valid_end_date, True, 1),
        (6, "LAZ", "Lazada", 20.0, 2015, today, default_valid_end_date, True, 3),
    ]

    # Initialize
    existing_scd_df = spark.createDataFrame(existing_data, existing_schema)
    source_df = spark.createDataFrame(source_data, source_schema)

    test_cases = [
        {
            "name": "Successfully generate correct SCD",
            "existing_scd_df": existing_scd_df,
            "expected": spark.createDataFrame(expected_data, existing_schema),
        },
        {
            "name": "Empty existing data -> populated source will be the new dim",
            "existing_scd_df": spark.createDataFrame(spark.sparkContext.emptyRDD(), existing_schema),
            "expected": spark.createDataFrame(
                [
                    (
                        1,
                        "SHP",
                        "Shopee",
                        10.0,
                        2015,
                        today,
                        default_valid_end_date,
                        True,
                        1,
                    ),
                    (
                        2,
                        "LAZ",
                        "Lazada",
                        20.0,
                        2015,
                        today,
                        default_valid_end_date,
                        True,
                        1,
                    ),
                    (
                        3,
                        "PHV",
                        "PhongVu",
                        10.0,
                        2015,
                        today,
                        default_valid_end_date,
                        True,
                        1,
                    ),
                ],
                existing_schema,
            ),
        },
    ]

    for test in test_cases:
        scd = ScdProcessor(
            natural_key="merchant_code",
            attribute_columns=["name", "rate", "est_year"],
            source_df=source_df,
            existing_scd_df=test["existing_scd_df"],
            id_generation_strategy=Hint.SINGLE_PARTITION,
        )
        actual_df = scd.run()
        # TODO: Temporary workaround
        # distributed natural of id generation unless we write a custom partitioner for this make id for each row
        # differs each run
        if test["existing_scd_df"].rdd.isEmpty():
            assert df_equals(actual_df.drop("id"), test["expected"].drop("id")), test["name"]
            assert sorted(actual_df.select("id").rdd.map(lambda x: x.asDict()["id"]).collect()) == [1, 2, 3]
        else:
            assert df_equals(actual_df, test["expected"]), test["name"]

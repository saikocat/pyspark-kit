from dataclasses import dataclass
from typing import List, Iterable

from pyspark.sql import Column, DataFrame, functions as F

from pyspark_kit.sdc import generate_sequential_ids, Hint


@dataclass
class ScdMetadataColumnName:
    surrogate_key: str = "id"
    valid_start_date: str = "scd_valid_start_date"
    valid_end_date: str = "scd_valid_end_date"
    version: str = "scd_version"
    is_active: str = "scd_is_active"


class ScdMetadata(object):
    """A helper class to generate right set of scd metadata column for each record type"""

    def __init__(self, column_names: ScdMetadataColumnName):
        self.names = column_names
        self.end_of_time_date = "9999-12-31"

    def for_updatable_records_columns(
        self, existing_version_col: Column
    ) -> List[Column]:
        """New entry/row for existing record that have updated attributes since update is not
        possible, we create a new entry/row:
        - `scd_valid_start_date` to current date
        - `scd_valid_end_date` to future date / end of time
        - `scd_is_active` to True as it is the most recent
        - `scd_version` is bump (+1)
        """
        return [
            F.current_date().alias(self.names.valid_start_date),
            F.to_date(F.lit(self.end_of_time_date)).alias(self.names.valid_end_date),
            F.lit(True).alias(self.names.is_active),
            existing_version_col + F.lit(1),
        ]

    def for_historical_records_columns(
        self, existing_valid_start_date_col: Column, existing_version_col: Column
    ) -> List[Column]:
        """Expire the previous current record
        - `scd_valid_end_date` to yesterday date
        - `scd_is_active` to False
        """
        return [
            existing_valid_start_date_col.alias(self.names.valid_start_date),
            F.date_sub(F.current_date(), 1).alias(self.names.valid_end_date),
            F.lit(False).alias(self.names.is_active),
            existing_version_col.alias(self.names.version),
        ]

    def for_new_records_columns(self) -> List[Column]:
        """Create new entry/rows for new record not seen in our existing
        - `scd_valid_start_date` to today
        - `scd_valid_end_date` to future date / end of time
        - `scd_is_active` to True
        - `scd_version` to 1
        """
        return [
            F.current_date().alias(self.names.valid_start_date),
            F.to_date(F.lit(self.end_of_time_date)).alias(self.names.valid_end_date),
            F.lit(True).alias(self.names.is_active),
            F.lit(1).alias(self.names.version),
        ]


class ScdProcessor(object):
    """ScdProcessor preserve history of changes - a dimension that stores and manages both current and historical data
    over time. It helps support "As Is" and "As Was" reporting.

    Examples:
        Given existing dim data with `id`: surrogate key, `merchant_code`: natural_key, `rate`: scd type2 attribute
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | id | merchant_code | name    | rate | est_year | scd_valid_start_date | scd_valid_end_date | scd_is_active | scd_version |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 1  | AMZ           | Amazon  | 10.0 | 2000     | 2020-01-25           | 9999-12-31         | True          | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 2  | LAZ           | Lazada  | 10.0 | 2015     | 2020-01-25           | 2020-01-26         | False         | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 3  | LAZ           | Lazada  | 15.0 | 2015     | 2020-01-26           | 9999-12-31         | True          | 2           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 4  | PHV           | PhongVu | 10.0 | 2015     | 2020-01-25           | 9999-12-31         | True          | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+

        New source data
        +---------------+---------+------+----------+
        | merchant_code | name    | rate | est_year |
        +---------------+---------+------+----------+
        | SHP           | Shopee  | 10.0 | 2015     | - New Merchant -> create new records
        ++--------------+---------+------+----------+
        | LAZ           | Lazada  | 20.0 | 2015     | - Rate attribute changed -> expire all records for natural key -> new row
        ++--------------+---------+------+----------+
        | PHV           | PhongVu | 10.0 | 2015     | - Attributes unchanged -> unaffected records (together with AMZ) -> keep all existings
        +---------------+---------+------+----------+

        End results when today is 2020-01-30
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | id | merchant_code | name    | rate | est_year | scd_valid_start_date | scd_valid_end_date | scd_is_active | scd_version |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 1  | AMZ           | Amazon  | 10.0 | 2000     | 2020-01-25           | 9999-12-31         | True          | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 2  | LAZ           | Lazada  | 10.0 | 2015     | 2020-01-25           | 2020-01-26         | False         | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 3  | LAZ           | Lazada  | 15.0 | 2015     | 2020-01-26           | 2020-01-29         | False         | 2           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 4  | PHV           | PhongVu | 10.0 | 2015     | 2020-01-25           | 9999-12-31         | True          | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 5  | SHP           | Shopee  | 10.0 | 2015     | 2020-01-30           | 9999-12-31         | True          | 1           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
        | 6  | LAZ           | Lazada  | 20.0 | 2010     | 2020-01-30           | 9999-12-31         | True          | 3           |
        +----+---------------+---------+------+----------+----------------------+--------------------+---------------+-------------+
    """

    def __init__(
        self,
        natural_key: str,
        attribute_columns: List[str],
        source_df: DataFrame,
        existing_scd_df: DataFrame = None,
        metadata_name: ScdMetadataColumnName = ScdMetadataColumnName(),
        id_generation_strategy: Hint = Hint.DISTRIBUTED,
    ):
        self.natural_key = natural_key
        self.surrogate_key = metadata_name.surrogate_key
        self.attribute_columns = attribute_columns
        self.metadata = ScdMetadata(metadata_name)
        self.source_df = source_df
        self.existing_scd_df = existing_scd_df
        self.id_generation_strategy = id_generation_strategy

        self.offset_id = self.find_offset(
            existing_scd_df, self.metadata.names.surrogate_key
        )

    def run(self) -> DataFrame:
        updated_records_df = self.create_updated_records()
        # force materialization
        updated_records_df.cache().count()
        modified_keys_df = self.find_modified_keys(updated_records_df)
        historical_records_df = self.create_historical_records(modified_keys_df)
        unaffected_records_df = self.find_unaffected_records(modified_keys_df)
        new_records_df = self.create_new_records()

        # Generate ID for new records and new entry for records required updates
        records_without_surrogate_key_df = new_records_df.union(
            # we drop the surrogate_key here cos it belongs to old record
            updated_records_df.drop(self.surrogate_key)
        )
        final_records_with_key_df = generate_sequential_ids(
            records_without_surrogate_key_df,
            offset=self.offset_id,
            id_col=self.metadata.names.surrogate_key,
            hint=self.id_generation_strategy,
        )

        # Combine all records together
        finalized = unaffected_records_df.union(historical_records_df).union(
            final_records_with_key_df
        )
        return finalized

    def create_updated_records(self) -> DataFrame:
        """Capture existing records that have attributes changed"""
        return self.source_df.join(
            self.existing_scd_df,
            on=[
                (
                    self.existing_scd_df[self.natural_key]
                    == self.source_df[self.natural_key]
                )
                & (self.existing_scd_df[self.metadata.names.is_active] == F.lit(True))
                & self.get_changed_attr_condition()
            ],
            how="inner",
        ).select(
            self.existing_scd_df[self.surrogate_key].alias(self.surrogate_key),
            self.existing_scd_df[self.natural_key].alias(self.natural_key),
            *self._get_columns_for_dataframe(self.source_df, self.attribute_columns),
            *self.metadata.for_updatable_records_columns(
                self.existing_scd_df[self.metadata.names.version]
            ),
        )

    def find_modified_keys(self, updatable_records_df: DataFrame) -> DataFrame:
        """Get only the keys/ids of those modified records"""
        return updatable_records_df.select(self.surrogate_key)

    def create_historical_records(self, modified_keys_df) -> DataFrame:
        """ Expire previous current records"""
        return self.existing_scd_df.join(
            modified_keys_df,
            on=[
                self.existing_scd_df[self.surrogate_key]
                == modified_keys_df[self.surrogate_key],
                self.existing_scd_df[self.metadata.names.is_active] == F.lit(True),
            ],
            how="inner",
        ).select(
            self.existing_scd_df[self.surrogate_key].alias(self.surrogate_key),
            self.existing_scd_df[self.natural_key].alias(self.natural_key),
            *self._get_columns_for_dataframe(
                self.existing_scd_df, self.attribute_columns
            ),
            *self.metadata.for_historical_records_columns(
                self.existing_scd_df[self.metadata.names.valid_start_date],
                self.existing_scd_df[self.metadata.names.version],
            ),
        )

    expire_records = create_historical_records

    def find_unaffected_records(self, modified_keys_df: DataFrame) -> DataFrame:
        """Isolate unaffected records that have no changes"""
        return self.existing_scd_df.join(
            modified_keys_df, on=[self.surrogate_key], how="left_anti",
        ).select(
            *map(lambda col: self.existing_scd_df[col], self.existing_scd_df.columns),
        )

    def create_new_records(self) -> DataFrame:
        """Create entries for completely new records"""
        return self.source_df.join(
            self.existing_scd_df, on=[self.natural_key], how="left_anti",
        ).select(
            self.source_df[self.natural_key].alias(self.natural_key),
            *self._get_columns_for_dataframe(self.source_df, self.attribute_columns),
            *self.metadata.for_new_records_columns(),
        )

    def find_max_id(self, existing_scd_df, scd_id_col_name):
        """Find the offset to use for sequential incr id

        Returns:
            offset_id if existing_scd_df is valid and non empty
            0 otherwise
        """
        if not existing_scd_df or existing_scd_df.rdd.isEmpty():
            return 0

        max_id_df = existing_scd_df.select(
            F.max(F.col(scd_id_col_name)).alias("max_id")
        )
        has_result = max_id_df.filter(F.isnull(F.col("max_id"))).count() == 0
        max_id = max_id_df.head()[0] if has_result else 0
        return max_id

    find_offset = find_max_id

    def get_changed_attr_condition(self) -> Column:
        """Generate the condition foreach attributes to detect whether the field is changed

        In SQL: (source.attr_a <> existing.attr_a OR source.attr_b <> existing.attr_b OR ...)
        In PyS: (source[attr_a] != existing.attr_a | source[attr_b] != existing.attr_b | ...)
        """
        condition = F.lit(False)
        for col in self.attribute_columns:
            condition = (self.source_df[col] != self.existing_scd_df[col]) | condition
        return condition

    def _get_columns_for_dataframe(
        self, df: DataFrame, attribute_columns: Iterable[str]
    ) -> Iterable[Column]:
        """Helper method to select the columns from specific df after using join"""
        return map(lambda col: df[col], attribute_columns)

from dataclasses import dataclass
from typing import List

from pyspark.sql import Column, functions as F


@dataclass
class ScdMetadataColumnName:
    valid_start_date: str = 'scd_valid_start_date'
    valid_end_date: str = 'scd_valid_end_date'
    version: str = 'scd_version'
    is_active: str = 'scd_is_active'


class ScdMetadata(object):
    def __init__(self, column_names: ScdMetadataColumnName):
        self.names = column_names
        self.end_of_time_date = '9999-12-31'

    def new_for_existing_records_columns(self, existing_version_col: Column) -> List[Column]:
        return [
            F.current_date().alias(self.names.valid_start_date),
            F.to_date(F.lit(self.end_of_time_date)).alias(self.names.valid_end_date),
            F.lit(True).alias(self.names.is_current),
            existing_version_col + F.lit(1),
        ]

    def new_historical_records_columns(self, existing_valid_start_date_col: Column, existing_version_col: Column) -> List[Column]:
        return [
            existing_valid_start_date_col.alias(self.names.valid_start_date),
            F.date_sub(F.current_date(), 1).alias(self.names.valid_end_date),
            F.lit(False).alias(self.names.is_active),
            existing_version_col.alias(self.names.version),
        ]

    def new_records_columns(self) -> List[Column]:
        return [
            F.current_date().alias(self.names.valid_start_date),
            F.to_date(F.lit(self.end_of_time_date)).alias(self.names.valid_end_date),
            F.lit(True).alias(self.names.is_active),
            F.lit(1).alias(self.names.version),
        ]


def find_max_id(existing_scd_df, scd_id_col_name):
    max_id_df = existing_scd_df.select(F.max(F.col(scd_id_col_name)).alias("max_id"))
    has_result = max_id_df.filter(F.isnull(F.col("max_id"))).count() == 0
    max_id = max_id_df.head()[0] if has_result else 0
    return max_id


find_offset_id = find_max_id


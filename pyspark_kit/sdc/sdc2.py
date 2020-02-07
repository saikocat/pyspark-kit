def find_max_id(existing_scd_df, scd_id_col_name):
    max_id_df = existing_scd_df.select(F.max(F.col(scd_id_col_name)).alias("max_id"))
    has_result = max_id_df.filter(F.isnull(F.col("max_id"))).count() == 0
    max_id = max_id_df.head()[0] if has_result else 0
    return max_id

find_offset_id = find_max_id


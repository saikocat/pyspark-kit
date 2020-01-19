# -*- coding: utf-8 -*-

from typing import Any, Callable, List

from cytoolz.functoolz import compose

from pyspark.sql.dataframe import DataFrame as SparkDataFrame


def chain_transforms(
    curried_funcs: List[Callable[[Any, SparkDataFrame], SparkDataFrame]],
    df: SparkDataFrame,
) -> SparkDataFrame:
    """Chain multiple transformations to the source dataframe

    Python API for DataFrame doesn't support .transforms() so this helper makes composing multiple transformations
    easier.

    Args:
        curried_funcs (List[Callable[[Any, SparkDataFrame], SparkDataFrame]]): List of curried functions (annotated
            with cytoolz.curry) each is a transformation for source DataFrame
        df (SparkDataFrame)

    Returns:
        dataframe after being applied all transformations

    Examples:
        >>> @curry
        >>> def with_label(label, df):
        >>>     return df.withColumn("label", lit(label))
        >>> ...
        >>> actual_df = chain_transforms([with_label("offline")], source_df)
    """
    chained = compose(*reversed(curried_funcs))
    return chained(df)

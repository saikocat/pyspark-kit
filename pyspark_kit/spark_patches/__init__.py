# -*- coding: utf-8 -*-
"""TODO:
- typing for params
- explain the mimic of scala API for .transform
- docs for module import
subpackage/__init__.py
import spark_patches
__all__ = ['spark_patches']
"""


def _transform(self, f):
    return f(self)


def _auto_patch():
    from pyspark.sql import dataframe as df

    df.DataFrame.transform = _transform


_auto_patch()

"""Cleaning and preprocessing functions for the raw TMDB Spark DataFrame (Phase 2)."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config


def drop_irrelevant_columns(df: DataFrame, columns: list[str] = config.COLUMNS_TO_DROP) -> DataFrame:
    """Return a new DataFrame without the columns the analysis does not need."""
    return df.drop(*columns)


def join_names(array_col: str, field: str = "name", sep: str = config.NAME_SEPARATOR) -> Column:
    """Join one field of an array-of-structs column into one string, e.g. 'Action|Drama'.

    Returns null (not an empty string) when the array is empty or missing.
    """
    names = F.col(f"{array_col}.{field}")              # e.g. ["Action", "Drama"]
    return F.when(F.size(names) > 0, F.concat_ws(sep, names))


def flatten_json_columns(df: DataFrame) -> DataFrame:
    """Replace the nested JSON-like columns with readable text columns."""
    new_columns = {"belongs_to_collection": F.col("belongs_to_collection.name")}
    for column, field in config.JSON_NAME_FIELDS.items():
        new_columns[column] = join_names(column, field)
    return df.withColumns(new_columns)                 # all replacements in one step
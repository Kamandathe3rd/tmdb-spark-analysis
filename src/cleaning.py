"""Cleaning and preprocessing functions for the raw TMDB Spark DataFrame (Phase 2)."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config


def drop_irrelevant_columns(df: DataFrame, columns: list[str] = config.COLUMNS_TO_DROP) -> DataFrame:
    """Return a new DataFrame without the columns the analysis does not need."""
    return df.drop(*columns)


def join_strings(names: Column, sep: str = config.NAME_SEPARATOR) -> Column:
    """Join an array of strings with sep, e.g. 'Action|Drama'; null if the array is empty or missing."""
    return F.when(F.size(names) > 0, F.concat_ws(sep, names))


def join_names(array_col: str, field: str = "name", sep: str = config.NAME_SEPARATOR) -> Column:
    """Join one field of an array-of-structs column, e.g. genres.name -> 'Action|Drama'."""
    return join_strings(F.col(f"{array_col}.{field}"), sep)


def flatten_json_columns(df: DataFrame) -> DataFrame:
    """Replace the nested JSON-like columns with readable text columns."""
    new_columns = {"belongs_to_collection": F.col("belongs_to_collection.name")}
    for column, field in config.JSON_NAME_FIELDS.items():
        new_columns[column] = join_names(column, field)
    return df.withColumns(new_columns)                 # all replacements in one step


def extract_credits(df: DataFrame) -> DataFrame:
    """Create cast, cast_size, director and crew_size from the nested credits column, then drop it."""
    directors = F.filter("credits.crew", lambda person: person["job"] == "Director")
    return (
        df.withColumns({
            "cast": join_names("credits.cast"),
            "cast_size": F.size("credits.cast"),
            "director": join_strings(F.transform(directors, lambda person: person["name"])),
            "crew_size": F.size("credits.crew"),
        })
        .drop("credits")
    )
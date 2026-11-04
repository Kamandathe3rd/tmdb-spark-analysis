"""Cleaning and preprocessing functions for the raw TMDB Spark DataFrame (Phase 2)."""

"""Cleaning and preprocessing functions for the raw TMDB Spark DataFrame (Phase 2)."""

import logging
import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config

logger = logging.getLogger(__name__)


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
def value_counts(df: DataFrame, column: str, split: bool = False,
                 sep: str = config.NAME_SEPARATOR) -> DataFrame:
    """Spark version of pandas value_counts(): how often each value appears (NULLs included).

    With split=True, "|"-separated text is split first, so "Action|Drama" counts once for each genre.
    """
    values = F.explode_outer(F.split(column, re.escape(sep))) if split else F.col(column)
    return (
        df.select(values.alias(column))
        .groupBy(column)
        .count()
        .orderBy(F.desc("count"), F.asc_nulls_last(column))
    )

def convert_datatypes(df: DataFrame) -> DataFrame:
    """Enforce numeric types and parse release_date as a real date.

    Invalid values become NULL (like pandas errors="coerce") instead of stopping the job.
    """
    new_columns = {column: F.col(column).try_cast(dtype) for column, dtype in config.COLUMN_TYPES.items()}
    new_columns["release_date"] = F.try_to_date("release_date", config.DATE_FORMAT)
    return df.withColumns(new_columns)

def replace_unrealistic_values(df: DataFrame, columns: list[str] = config.ZERO_MEANS_MISSING) -> DataFrame:
    """Turn 0 (or negative) budget, revenue and runtime into NULL: TMDB uses 0 for 'unknown'."""
    return df.withColumns({column: F.when(F.col(column) > 0, F.col(column)) for column in columns})


def convert_to_millions(df: DataFrame) -> DataFrame:
    """Add budget_musd and revenue_musd (million USD) and drop the original dollar columns."""
    return (
        df.withColumns({
            "budget_musd": F.col("budget") / config.MILLION,
            "revenue_musd": F.col("revenue") / config.MILLION,
        })
        .drop("budget", "revenue")
    )

def clear_unrated_vote_average(df: DataFrame) -> DataFrame:
    """A vote_average with no votes behind it is meaningless, so set it to NULL when vote_count is 0."""
    return df.withColumn("vote_average", F.when(F.col("vote_count") > 0, F.col("vote_average")))


def replace_text_placeholders(df: DataFrame, columns: list[str] = config.TEXT_COLUMNS) -> DataFrame:
    """Replace placeholder text such as 'No Data' or empty strings with NULL (and trim spaces)."""
    def clean(column: str) -> Column:
        text = F.trim(F.col(column))
        return F.when(~F.lower(text).isin(config.TEXT_PLACEHOLDERS), text)

    return df.withColumns({column: clean(column) for column in columns})

def remove_bad_rows(df: DataFrame) -> DataFrame:
    """Drop rows missing id/title, duplicate movies (same id) and rows with too few values.

    Logs the row count after each rule, as evidence of what was removed.
    """
    logger.info("Rows at start: %d", df.count())

    df = df.dropna(subset=config.REQUIRED_COLUMNS)          # first, so NULL ids are not "deduplicated"
    logger.info("After dropping rows without %s: %d", config.REQUIRED_COLUMNS, df.count())

    df = df.dropDuplicates(["id"])                           # same movie fetched twice -> keep one
    logger.info("After removing duplicate ids: %d", df.count())

    df = df.dropna(thresh=config.MIN_NON_NULL_VALUES)        # keep rows with >= 10 non-null values
    logger.info("After keeping rows with >= %d non-null values: %d", config.MIN_NON_NULL_VALUES, df.count())
    return df

def keep_released_movies(df: DataFrame) -> DataFrame:
    """Keep only movies whose status is 'Released', then drop the status column."""
    total = df.count()
    df = df.filter(F.col("status") == config.RELEASED_STATUS).drop("status")
    logger.info("Kept %d of %d movies with status '%s'", df.count(), total, config.RELEASED_STATUS)
    return df

def select_final_columns(df: DataFrame, columns: list[str] = config.FINAL_COLUMNS) -> DataFrame:
    """Return only the final columns, in the assignment's order; stop clearly if any is missing."""
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Columns missing before final selection: {missing}")

    left_out = sorted(set(df.columns) - set(columns))
    logger.info("Not in the final layout (dropped): %s", left_out)
    return df.select(*columns)
"""Cleaning and preprocessing functions for the raw TMDB Spark DataFrame (Phase 2)."""

from pyspark.sql import DataFrame

from src import config


def drop_irrelevant_columns(df: DataFrame, columns: list[str] = config.COLUMNS_TO_DROP) -> DataFrame:
    """Return a new DataFrame without the columns the analysis does not need."""
    return df.drop(*columns)
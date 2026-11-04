"""Data preparation pipeline: fetch (or reuse) raw TMDB data -> clean it -> save the checkpoint.

Run from the project root with:   python -m src.pipeline            (reuse saved raw data)
                                  python -m src.pipeline --fetch    (call the TMDB API first)
The notebook calls run() in a single cell, so the same log lines appear there as evidence.
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession

from src import cleaning, config, fetch, kpis

logger = logging.getLogger(__name__)

# Phase 2 cleaning steps, in order: (description, function)
CLEANING_STEPS = [
    ("Drop irrelevant columns", cleaning.drop_irrelevant_columns),
    ("Flatten JSON-like columns", cleaning.flatten_json_columns),
    ("Extract cast, director and crew sizes", cleaning.extract_credits),
    ("Convert datatypes", cleaning.convert_datatypes),
    ("Replace unrealistic 0 values with NULL", cleaning.replace_unrealistic_values),
    ("Convert budget and revenue to million USD", cleaning.convert_to_millions),
    ("Clear ratings with zero votes", cleaning.clear_unrated_vote_average),
    ("Replace placeholder text with NULL", cleaning.replace_text_placeholders),
    ("Remove bad rows", cleaning.remove_bad_rows),
    ("Keep released movies only", cleaning.keep_released_movies),
    ("Select final columns in required order", cleaning.select_final_columns),
]


def create_spark_session(app_name: str = "tmdb-analysis") -> SparkSession:
    """Start (or reuse) a local SparkSession whose Python workers use this project's .venv."""
    os.environ["PYSPARK_PYTHON"] = sys.executable          # not the Microsoft Store "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")       # default 200 is far too many for 18 rows
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_raw_data(spark: SparkSession, fetch_from_api: bool = False) -> DataFrame:
    """Fetch from TMDB if asked (or if no saved file exists), then read the raw JSON into Spark."""
    if fetch_from_api or not config.RAW_MOVIES_FILE.exists():
        fetch.save_raw_json(fetch.fetch_all_movies())
    else:
        logger.info("Using saved raw data: %s", config.RAW_MOVIES_FILE)
    return spark.read.json(config.RAW_MOVIES_FILE.as_posix(), multiLine=True)


def clean_movies(raw_df: DataFrame) -> DataFrame:
    """Run every Phase 2 cleaning step in order, logging the column count after each one."""
    df = raw_df
    for number, (description, step) in enumerate(CLEANING_STEPS, start=1):
        df = step(df)
        logger.info("Step %d/%d %s -> %d columns", number, len(CLEANING_STEPS), description, len(df.columns))
    return df


def run(spark: SparkSession, fetch_from_api: bool = False) -> DataFrame:
    """Full data preparation. Returns the cleaned movies with profit_musd and roi, ready for analysis."""
    raw_df = load_raw_data(spark, fetch_from_api)
    logger.info("Raw data: %d movies x %d columns", raw_df.count(), len(raw_df.columns))

    cleaning.save_clean_data(clean_movies(raw_df))

    movies_df = kpis.add_financial_metrics(cleaning.load_clean_data(spark))   # analysis starts from the checkpoint
    logger.info("Ready for analysis: %d movies x %d columns", movies_df.count(), len(movies_df.columns))
    return movies_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    run(create_spark_session(), fetch_from_api="--fetch" in sys.argv)
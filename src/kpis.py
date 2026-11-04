"""KPI calculations and rankings for the cleaned TMDB movies (Phase 3)."""

import re

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src import config


def add_financial_metrics(df: DataFrame) -> DataFrame:
    """Add profit_musd (revenue - budget) and roi (revenue / budget).

    This is the ONLY place profit and ROI are calculated; every later step reuses these columns.
    Both are NULL when budget or revenue is unknown; try_divide also returns NULL (not a crash) for a 0 budget.
    """
    return df.withColumns({
        "profit_musd": F.col("revenue_musd") - F.col("budget_musd"),
        "roi": F.try_divide(F.col("revenue_musd"), F.col("budget_musd")),
    })


@F.udf(returnType=StringType())
def format_metric(value, metric):
    """Spark UDF: turn a number into readable text for ranking tables, e.g. '$2.92B', '12.34x', '8.2/10'.

    No type hints here on purpose: PySpark warns when a UDF's function has them.
    """
    if value is None:
        return None
    if metric.endswith("_musd"):                       # money in million USD
        sign = "-" if value < 0 else ""
        amount = abs(value)
        return f"{sign}${amount / 1000:.2f}B" if amount >= 1000 else f"{sign}${amount:.1f}M"
    if metric == "roi":
        return f"{value:.2f}x"
    if metric == "vote_average":
        return f"{value:.1f}/10"
    if metric == "vote_count":
        return f"{value:,.0f}"
    return f"{value:,.2f}"


def rank_movies(df: DataFrame, metric: str, ascending: bool = False, top_n: int = 5,
                condition: Column | None = None) -> DataFrame:
    """Rank movies by one metric: best first (ascending=False) or worst first (ascending=True).

    Movies with a NULL metric are skipped; `condition` adds an extra filter (e.g. budget >= 10M).
    Ties share the same rank. A readable `display` column is added with the format_metric UDF.
    """
    ranked = df.filter(F.col(metric).isNotNull())
    if condition is not None:
        ranked = ranked.filter(condition)

    order = F.col(metric).asc() if ascending else F.col(metric).desc()
    return (
        ranked.withColumn("rank", F.rank().over(Window.orderBy(order)))
        .filter(F.col("rank") <= top_n)
        .select("rank", "title", metric, format_metric(F.col(metric), F.lit(metric)).alias("display"))
        .orderBy("rank", "title")
    )


def has_item(column: str, value: str, sep: str = config.NAME_SEPARATOR) -> Column:
    """True if `value` is one of the sep-separated items in `column` (exact match, not a substring)."""
    return F.array_contains(F.split(F.col(column), re.escape(sep)), value)


def search_movies(df: DataFrame, genres: list[str] | None = None, cast: list[str] | None = None,
                  director: str | None = None, sort_by: str = "vote_average",
                  ascending: bool = False) -> DataFrame:
    """Find movies that have ALL the given genres and cast members (and the director, if given)."""
    condition = F.lit(True)
    for genre in genres or []:
        condition &= has_item("genres", genre)
    for person in cast or []:
        condition &= has_item("cast", person)
    if director is not None:
        condition &= has_item("director", director)

    order = F.col(sort_by).asc() if ascending else F.col(sort_by).desc()
    return (
        df.filter(condition)
        .select("title", "genres", "director", "vote_average", "runtime")
        .orderBy(order, "title")
    )
def add_movie_type(df: DataFrame) -> DataFrame:
    """Label each movie 'Franchise' (belongs to a collection) or 'Standalone' (no collection)."""
    return df.withColumn(
        "movie_type",
        F.when(F.col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone"),
    )


def compare_franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """Compare franchise and standalone movies: mean revenue, median ROI, mean budget, popularity, rating.

    Results are rounded to 2 decimals because this is a final summary table.
    """
    return (
        add_movie_type(df)
        .groupBy("movie_type")
        .agg(
            F.count("*").alias("movies"),
            F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
            F.round(F.median("roi"), 2).alias("median_roi"),
            F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
            F.round(F.mean("popularity"), 2).alias("mean_popularity"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy("movie_type")
    )

def top_franchises(df: DataFrame, top_n: int = 10) -> DataFrame:
    """Most successful franchises (collections), ranked by total revenue.

    Shows number of movies, total & mean budget, total & mean revenue and mean rating.
    """
    return (
        df.filter(F.col("belongs_to_collection").isNotNull())
        .groupBy(F.col("belongs_to_collection").alias("franchise"))
        .agg(
            F.count("*").alias("movies"),
            F.round(F.sum("budget_musd"), 2).alias("total_budget_musd"),
            F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue_musd"), "franchise")
        .limit(top_n)
    )
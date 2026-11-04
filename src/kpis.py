"""KPI calculations and rankings for the cleaned TMDB movies (Phase 3)."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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
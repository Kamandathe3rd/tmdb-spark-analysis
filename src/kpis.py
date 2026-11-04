"""KPI calculations and rankings for the cleaned TMDB movies (Phase 3)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_financial_metrics(df: DataFrame) -> DataFrame:
    """Add profit_musd (revenue - budget) and roi (revenue / budget).

    This is the ONLY place profit and ROI are calculated; every later step reuses these columns.
    Both are NULL when budget or revenue is unknown; try_divide also returns NULL (not a crash) for a 0 budget.
    """
    return df.withColumns({
        "profit_musd": F.col("revenue_musd") - F.col("budget_musd"),
        "roi": F.try_divide(F.col("revenue_musd"), F.col("budget_musd")),
    })
"""Matplotlib charts for the TMDB analysis (Phase 4).

Each function draws ONE chart from a small pandas DataFrame, saves it to docs/figures/ and shows it.
Spark does the data preparation in the notebook; only the small, aggregated result is passed here.
"""

import matplotlib.pyplot as plt
import pandas as pd

from src import config

# One shared look for every chart (colours from a colour-blind-checked palette)
COLOR_MAIN = "#2a78d6"     # blue   - main series / franchise
COLOR_ACCENT = "#eb6834"   # orange - second series / standalone / outliers
COLOR_TEXT = "#52514e"     # grey   - secondary text and reference lines

plt.rcParams.update({
    "figure.figsize": (9, 5.5),
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.titleweight": "bold",
    "axes.titlesize": 13,
    "font.size": 10,
})


def _finish(fig: plt.Figure, filename: str) -> None:
    """Tidy the layout, save the chart as a PNG in docs/figures/ and show it (one show per chart)."""
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(config.FIGURES_DIR / filename, dpi=150)
    plt.show()


def plot_revenue_vs_budget(pdf: pd.DataFrame, label_top: int = 5) -> None:
    """Chart 1: scatter of budget vs revenue with a break-even line; the top earners are labelled."""
    fig, ax = plt.subplots()
    ax.scatter(pdf["budget_musd"], pdf["revenue_musd"], s=60, color=COLOR_MAIN,
               edgecolor="white", linewidth=1.5, zorder=3)

    # Break-even line: points above it made more than they cost
    limit = max(pdf["budget_musd"].max(), pdf["revenue_musd"].max()) * 1.05
    ax.plot([0, limit], [0, limit], linestyle="--", color=COLOR_TEXT, linewidth=1,
            label="Break-even (revenue = budget)")

    top = pdf.nlargest(label_top, "revenue_musd").reset_index(drop=True)
    for i, row in top.iterrows():
        offset = (6, 6) if i % 2 == 0 else (6, -12)     # alternate above/below so close labels don't overlap
        ax.annotate(row["title"], (row["budget_musd"], row["revenue_musd"]),
                    xytext=offset, textcoords="offset points", fontsize=8, color=COLOR_TEXT)

    ax.set(title="Revenue vs Budget", xlabel="Budget (million USD)", ylabel="Revenue (million USD)",
           xlim=(0, pdf["budget_musd"].max() * 1.15), ylim=(0, limit))
    ax.legend(loc="lower right", frameon=False)
    _finish(fig, "01_revenue_vs_budget.png")


def plot_roi_by_genre(pdf: pd.DataFrame) -> None:
    """Chart 2: box plot of ROI per genre (one row per movie-genre pair), sorted by median ROI."""
    order = pdf.groupby("genre")["roi"].median().sort_values().index
    groups = [pdf.loc[pdf["genre"] == genre, "roi"] for genre in order]
    labels = [f"{genre} (n={len(values)})" for genre, values in zip(order, groups)]

    fig, ax = plt.subplots()
    ax.boxplot(groups, orientation="horizontal", patch_artist=True, widths=0.6,
               boxprops={"facecolor": COLOR_MAIN, "alpha": 0.35, "edgecolor": COLOR_MAIN},
               medianprops={"color": COLOR_MAIN, "linewidth": 2},
               whiskerprops={"color": COLOR_TEXT}, capprops={"color": COLOR_TEXT},
               flierprops={"markerfacecolor": COLOR_ACCENT, "markeredgecolor": "white", "markersize": 7})
    ax.set_yticks(range(1, len(labels) + 1), labels)
    ax.set(title="ROI Distribution by Genre", xlabel="ROI (revenue ÷ budget)", ylabel="")
    ax.grid(axis="y", visible=False)
    _finish(fig, "02_roi_by_genre.png")


def plot_popularity_vs_rating(pdf: pd.DataFrame, label_top: int = 3) -> None:
    """Chart 3: scatter of rating vs popularity; the most popular films are labelled."""
    fig, ax = plt.subplots()
    ax.scatter(pdf["vote_average"], pdf["popularity"], s=60, color=COLOR_MAIN,
               edgecolor="white", linewidth=1.5, zorder=3)

    top = pdf.nlargest(label_top, "popularity").reset_index(drop=True)
    for i, row in top.iterrows():
        offset = (6, 6) if i % 2 == 0 else (6, -12)
        ax.annotate(row["title"], (row["vote_average"], row["popularity"]),
                    xytext=offset, textcoords="offset points", fontsize=8, color=COLOR_TEXT)

    ax.margins(x=0.15)                         # room on the right so labels are not cut off
    ax.set(title="Popularity vs Rating", xlabel="Average rating (out of 10)", ylabel="TMDB popularity score")
    _finish(fig, "03_popularity_vs_rating.png")


def plot_yearly_box_office(pdf: pd.DataFrame) -> None:
    """Chart 4: total revenue per release year; each bar is labelled with its number of films."""
    fig, ax = plt.subplots()
    years = pdf["year"].astype(str)            # years as labels, so gaps between years are not drawn
    bars = ax.bar(years, pdf["total_revenue_musd"], color=COLOR_MAIN, width=0.6, zorder=3)

    for bar, films in zip(bars, pdf["movies"]):
        ax.annotate(f"{films} film{'s' if films != 1 else ''}",
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, 4), textcoords="offset points", ha="center", fontsize=8, color=COLOR_TEXT)

    ax.set(title="Yearly Box Office: Total Revenue by Release Year",
           xlabel="Release year", ylabel="Total revenue (million USD)")
    ax.grid(axis="x", visible=False)
    _finish(fig, "04_yearly_box_office.png")


def plot_franchise_vs_standalone(pdf: pd.DataFrame) -> None:
    """Chart 5: small multiples, one bar panel per metric, comparing franchise and standalone films.

    Separate panels (not one chart with two y-axes) because the metrics use different units.
    """
    metrics = [
        ("mean_revenue_musd", "Mean revenue ($M)"),
        ("median_roi", "Median ROI (x)"),
        ("mean_budget_musd", "Mean budget ($M)"),
        ("mean_rating", "Mean rating (/10)"),
    ]
    labels = [f"{t}\n(n={n})" for t, n in zip(pdf["movie_type"], pdf["movies"])]
    colors = [COLOR_MAIN if t == "Franchise" else COLOR_ACCENT for t in pdf["movie_type"]]

    fig, axes = plt.subplots(1, len(metrics), figsize=(12, 4.5))
    for ax, (column, title) in zip(axes, metrics):
        bars = ax.bar(labels, pdf[column], color=colors, width=0.6, zorder=3)
        ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=9, color=COLOR_TEXT)
        ax.set_title(title, fontsize=11)
        ax.set_ylim(0, pdf[column].max() * 1.2)
        ax.grid(axis="x", visible=False)

    fig.suptitle("Franchise vs Standalone Movies", fontweight="bold", fontsize=13)
    _finish(fig, "05_franchise_vs_standalone.png")
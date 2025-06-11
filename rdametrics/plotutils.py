"""
PLOT UTILS
"""

import pandas as pd
import matplotlib.pyplot as plt


def plot_xy(df: pd.DataFrame, x: str, y: str, title="Scatter Plot"):
    """Create a scatter plot with the given x and y data."""

    plt.subplot(1, 2, 2)
    plt.scatter(df[x], df[y], alpha=0.7, color="black", s=1)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(title)

    plt.tight_layout()
    plt.show()


### END ###

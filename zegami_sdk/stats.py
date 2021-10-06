# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""stats functionality."""

import pandas as pd


def calc_num_correlation_matrix(df):
    """Calculates a matrix of correlations for all numerical pairs of columns in a collection."""
    # Get the numeric columns
    num_cols = [c for c in df.columns if df[c].dtype.kind.lower() in ['f', 'i']]

    # Make into reduced frame
    df_num = df[num_cols]

    # Normalize
    df_num_norm = (df_num - df_num.mean(skipna=True)) / df_num.std(skipna=True)

    return df_num_norm.cov()


def calc_num_summary(df):
    """Calculates a table to summarise the numeric columns of a collection.

    Includes:
        - Mean
        - Median
        - Range
        - Standard deviation
    """
    # Get the numeric columns
    num_cols = [c for c in df.columns if df[c].dtype.kind.lower() in ['f', 'i']]
    df = df[num_cols]

    # Calculate the means
    means = [df[col].mean(skipna=True) for col in df.columns]

    # Calculate the medians
    medians = [df[col].median(skipna=True) for col in df.columns]

    # Calculate the min, max, range
    mins = [df[col].min(skipna=True) for col in df.columns]
    maxs = [df[col].max(skipna=True) for col in df.columns]
    ranges = [maxs[i] - mins[i] for i in range(len(mins))]

    # Calculate the standard deviations
    stds = [df[col].std(skipna=True) for col in df.columns]

    # Construct the results table
    df_out = pd.DataFrame(
        data=[means, medians, mins, maxs, ranges, stds],
        columns=df.columns
    )

    df_out.index = ['Mean', 'Median', 'Min', 'Max', 'Range',
                    'Standard Deviation']

    return df_out


def calc_cat_representations(df, columns=None, max_cardinality=None):
    """Calculates the 'representation' for a categorical column.

    A score closer to zero means that values in the column are more skewed
    towards certain classes (some are being under-represented). If closer to
    one, there is a more even distribution of possible values.

    To specify only certain columns (case sensitive) to analyse, use
    columns=['MyColumnA', 'MyColumnB']. Using None will look at all valid
    columns.

    Columns who's unique values exceed 'max_cardinality' are also excluded to
    avoid looking at columns likely containing many mostly unique strings.
    If a column should have many classes, increase this number.
    To subdue this behaviour entirely, use 'max_cardinality=None'.

    Columns whose result is nan are excluded from the output.
    """
    # Get all object columns
    cat_cols = [col for col in df.columns if df[col].dtype.kind == 'O']

    # If filtering to specific columns, exclude any that don't match
    if columns is not None:
        if not type(columns) is list:
            columns = [columns]
        cat_cols = [col for col in cat_cols if col in columns]

    # Exclude high-cardinality columns
    if max_cardinality is not None:
        cat_cols = [col for col in cat_cols if len(set(df[col])) <= max_cardinality]

    # Build the representation score for each valid column
    rep_scores = []
    for col in cat_cols:

        # The number of unique classes in the column
        unique_classes = df[col].nunique()

        # The count per unique class in the column
        class_counts = df[col].value_counts()

        # The total samples (should be ~len(rows))
        total_counts = class_counts.sum(skipna=True)

        # Ideal count per class
        ideal_per_class = total_counts / unique_classes

        # Normalized counts per class
        norm_class_counts = (class_counts - ideal_per_class).abs() / class_counts.std(skipna=True)

        # The representation score
        rep_score = 1 - norm_class_counts.std(skipna=True)

        rep_scores.append(rep_score)

    return {
        cat_cols[i]: max(0, rep_scores[i]) for i in range(len(cat_cols)) if not pd.isna(rep_scores[i])
    }

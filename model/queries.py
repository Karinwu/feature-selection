from typing import Dict, List, Optional, Sequence

import pandas as pd

import feature_selection.data.constants as c


def query_premise_training_data(
    adoption_feature_cols: List[str],
    index_col: Optional[str],
    dataset: str,
    samples_frac: Optional[float] = None,
) -> pd.DataFrame:
    """
    Function to query random sample of premises from bigquery, remove
    duplicated rows and set premise_id as index.
    Args:
        adoption_feature_cols: adoption feature columns
        index_col: column to use as index. default set to "premise_id"
        dataset: dataset to query.
        samples_frac: Optionally pass a fraction of samples to query
    """
    if index_col is None:
        index_col = c.PREMISE_INDEX_COL
    samples = samples_frac * c.FRAC_TO_PERCENT
    samples_clause = (
        ""
        if samples is None
        else (
            f" WHERE ABS(MOD(FARM_FINGERPRINT(CAST({index_col} AS STRING)), "
            f"100)) < {samples}"
        )
    )
    utilities = ["utility_1", "utility_2", "utility_3"]
    if dataset.lower() in utilities:
        premise_table = f"project.{dataset}.premises_der".lower()
        query = f"""
            WITH TRAINING_SET AS
            (
            SELECT
            {index_col},
            {", ".join(adoption_feature_cols)},
            FROM {premise_table}     
            )
            SELECT * FROM TRAINING_SET{samples_clause}
            """
    else:
        raise KeyError(
            f"{dataset} is not valid. Must be one of:"
            f" {', '.join(utilities)}"
        )

    # download table from bigquery
    return (
        pd.read_gbq(
            query, project_id="project", use_bqstorage_api=True
        )
        .drop_duplicates()
        .set_index(index_col)
    )


def query_training_data(
    feeder_ids: Optional[Sequence[str]] = None,
) -> str:
    table_id = "project.input.feeders_data"
    select_join = f"""
    SELECT
        *
    FROM
        `{table_id}` p    
    """
    if feeder_ids is None:
        return f"{select_join}"
    elif len(feeder_ids) == 1:
        return f'{select_join} WHERE feeder_id = "{feeder_ids[0]}"'
    else:
        return f"{select_join} WHERE feeder_id IN {tuple(feeder_ids)}"

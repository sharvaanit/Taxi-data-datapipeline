"""
Tests for pivot utilities: column detection, path helpers, pivot, cleanup, infer_month_from_path.
"""

import pytest
import pandas as pd

try:
    from .pivot_utils import (
        find_pickup_datetime_col,
        find_pickup_location_col,
        infer_taxi_type_from_path,
        infer_month_from_path,
        pivot_counts_date_taxi_type_location,
        cleanup_low_count_rows,
        is_s3_path,
        get_storage_options,
    )
    from .partition_optimization import parse_size
except ImportError:
    from pivot_utils import (
        find_pickup_datetime_col,
        find_pickup_location_col,
        infer_taxi_type_from_path,
        infer_month_from_path,
        pivot_counts_date_taxi_type_location,
        cleanup_low_count_rows,
        is_s3_path,
        get_storage_options,
    )
    from partition_optimization import parse_size


# --- Column detection ---


def test_find_pickup_datetime_col_dataframe():
    df = pd.DataFrame({"tpep_pickup_datetime": [1], "PULocationID": [2]})
    assert find_pickup_datetime_col(df) == "tpep_pickup_datetime"


def test_find_pickup_datetime_col_pickup_datetime():
    df = pd.DataFrame({"pickup_datetime": [1], "other": [2]})
    assert find_pickup_datetime_col(df) == "pickup_datetime"


def test_find_pickup_datetime_col_case_insensitive():
    df = pd.DataFrame({"Tpep_Pickup_DateTime": [1]})
    assert find_pickup_datetime_col(df) == "Tpep_Pickup_DateTime"


def test_find_pickup_datetime_col_missing():
    df = pd.DataFrame({"foo": [1], "bar": [2]})
    assert find_pickup_datetime_col(df) is None


def test_find_pickup_location_col_pulocationid():
    df = pd.DataFrame({"PULocationID": [1], "tpep_pickup_datetime": [2]})
    assert find_pickup_location_col(df) == "PULocationID"


def test_find_pickup_location_col_pickup_location_id():
    df = pd.DataFrame({"pickup_location_id": [1]})
    assert find_pickup_location_col(df) == "pickup_location_id"


def test_find_pickup_location_col_missing():
    df = pd.DataFrame({"foo": [1]})
    assert find_pickup_location_col(df) is None


# --- Path helpers ---


def test_infer_taxi_type_from_path_yellow():
    assert infer_taxi_type_from_path("yellow_tripdata_2023-01.parquet") == "yellow"
    assert infer_taxi_type_from_path("/path/yellow_tripdata_2023-01.parquet") == "yellow"


def test_infer_taxi_type_from_path_green():
    assert infer_taxi_type_from_path("green_tripdata_2023-01.parquet") == "green"


def test_infer_taxi_type_from_path_fhv():
    assert infer_taxi_type_from_path("fhv_tripdata_2023-01.parquet") == "fhv"


def test_infer_month_from_path_yyyymm():
    assert infer_month_from_path("yellow_tripdata_2023-01.parquet") == (2023, 1)
    assert infer_month_from_path("/data/yellow_tripdata_2023-12.parquet") == (2023, 12)


def test_infer_month_from_path_year_month():
    assert infer_month_from_path("s3://bucket/year=2023/month=01/file.parquet") == (2023, 1)
    assert infer_month_from_path("year=2022/month=6/data.parquet") == (2022, 6)


def test_infer_month_from_path_invalid():
    assert infer_month_from_path("random_file.parquet") is None


# --- Pivot ---


def test_pivot_counts_date_taxi_type_location_basic():
    pdf = pd.DataFrame({
        "taxi_type": ["yellow", "yellow", "yellow"],
        "date": [pd.Timestamp("2023-01-01").date()] * 3,
        "pickup_place": ["1", "1", "2"],
        "hour": [0, 1, 0],
        "count": [10, 20, 5],
    })
    out = pivot_counts_date_taxi_type_location(pdf)
    assert "taxi_type" in out.columns and "date" in out.columns and "pickup_place" in out.columns
    assert all(f"hour_{h}" in out.columns for h in range(24))
    assert len(out) >= 1
    assert out["hour_0"].sum() >= 15
    assert out["hour_1"].sum() >= 20


def test_pivot_counts_date_taxi_type_location_empty():
    pdf = pd.DataFrame(columns=["taxi_type", "date", "pickup_place", "hour", "count"])
    out = pivot_counts_date_taxi_type_location(pdf)
    assert list(out.columns) == ["taxi_type", "date", "pickup_place"] + [f"hour_{h}" for h in range(24)]
    assert len(out) == 0


# --- Cleanup ---


def test_cleanup_low_count_rows_keeps_above():
    df = pd.DataFrame({
        "taxi_type": ["yellow", "yellow"],
        "date": [pd.Timestamp("2023-01-01").date()] * 2,
        "pickup_place": ["1", "2"],
        **{f"hour_{h}": [0] * 2 for h in range(24)},
    })
    df.loc[0, "hour_0"] = 60
    df.loc[1, "hour_1"] = 100
    kept, stats = cleanup_low_count_rows(df, min_rides=50)
    assert stats["kept"] >= 1
    assert stats["dropped"] >= 0


def test_cleanup_low_count_rows_drops_below():
    df = pd.DataFrame({
        "taxi_type": ["yellow"],
        "date": [pd.Timestamp("2023-01-01").date()],
        "pickup_place": ["1"],
        **{f"hour_{h}": [0] for h in range(24)},
    })
    df["hour_0"] = 10
    kept, stats = cleanup_low_count_rows(df, min_rides=50)
    assert len(kept) == 0
    assert stats["dropped"] == 1


# --- S3 helpers ---


def test_is_s3_path():
    assert is_s3_path("s3://bucket/key") is True
    assert is_s3_path("S3://bucket/key") is True
    assert is_s3_path("/local/path") is False
    assert is_s3_path("file.parquet") is False


def test_get_storage_options_s3():
    opts = get_storage_options("s3://bucket/key")
    assert isinstance(opts, dict)


# --- parse_size ---


def test_parse_size_mb():
    assert parse_size("200MB") == 200 * 1024 * 1024
    assert parse_size("1.5GB") == int(1.5 * 1024**3)


def test_parse_size_gb():
    assert parse_size("1GB") == 1024**3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

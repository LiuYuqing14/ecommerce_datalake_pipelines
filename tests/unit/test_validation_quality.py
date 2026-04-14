import pandas as pd

from src.validation import quality


def test_require_columns_and_format_reject_rate():
    missing = quality.require_columns(["a", "b"], ["b", "c"])
    assert missing == ["c"]
    assert quality.format_reject_rate(0, 0) == 0.0
    assert quality.format_reject_rate(2, 4) == 0.5


def test_validate_table_flags_missing_and_null_pk():
    df = pd.DataFrame(
        {
            "id": [1, None, 3],
            "value": [10, 20, 30],
        }
    )
    failures = quality.validate_table(
        df,
        {
            "required_columns": ["id", "value", "missing"],
            "primary_key": ["id"],
        },
    )
    assert any("missing_required_columns" in f for f in failures)
    assert any("primary_key_nulls" in f for f in failures)


def test_split_fk_with_allow_prefixes():
    df = pd.DataFrame({"cust_id": ["c_1", "tmp_2", "c_3"]})
    ref = pd.DataFrame({"customer_id": ["c_1", "c_3"]})
    kept, dropped = quality.split_fk(
        df, "cust_id", ref, "customer_id", allow_prefixes=["tmp_"]
    )
    assert list(kept["cust_id"]) == ["c_1", "tmp_2", "c_3"]
    assert dropped.empty


def test_evaluate_expectations():
    df = pd.DataFrame({"a": [1, None, 3], "b": [1, 1, 1], "c": [5, 6, 7]})
    failures = quality.evaluate_expectations(
        df,
        [
            {"type": "not_null", "columns": ["a"]},
            {"type": "unique", "columns": ["b"]},
            {"type": "between", "column": "c", "min": 6, "max": 7},
            {"type": "in_set", "column": "a", "allowed": [1, 3]},
            {"type": "unknown"},
        ],
    )
    assert any("expect_not_null_failed" in f for f in failures)
    assert any("expect_unique_failed" in f for f in failures)
    assert any("expect_between_failed" in f for f in failures)
    assert any("expect_in_set_failed" in f for f in failures)
    assert any("unknown_expectation_type" in f for f in failures)


def test_split_fk_handles_missing_columns():
    df = pd.DataFrame({"a": [1, 2]})
    ref = pd.DataFrame({"b": [1, 2]})
    kept, dropped = quality.split_fk(df, "missing", ref, "b")
    assert kept.equals(df)
    assert dropped.empty

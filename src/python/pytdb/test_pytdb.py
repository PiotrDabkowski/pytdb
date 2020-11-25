from .pytdb import Table
import pandas as pd


def test_pytdb_db_creation(tmpdir):
    db = Table(tmpdir, "StockPrices", time_column="time", tag_columns=["symbol"], float_value_columns=["price"])
    df = pd.DataFrame(dict(time=[1,2,3], price=[11,22,33], symbol=["GOOG", "FB", "FB"]))
    db.append_data_df(df)

def test_pytdb_db_queries(tmpdir):
    db = Table(tmpdir, "StockPrices", time_column="time", tag_columns=["symbol"], float_value_columns=["price"])
    df = pd.DataFrame(dict(time=[1,2,3], price=[11,22,33], symbol=["GOOG", "FB", "FB"]))
    db.append_data_df(df)
    assert list(db.query_df(symbol=["FB"]).symbol) == [ "FB", "FB"]
    assert list(db.query_df(symbol=["GOOG"]).symbol) == [ "GOOG" ]
    assert sorted(list(db.query_df().symbol)) == [ "FB", "FB", "GOOG"]
    assert sorted(list(db.query_df(last_n=1).symbol)) == [ "FB", "GOOG"]
    assert sorted(list(db.query_df(time_end=2).symbol)) == [ "GOOG"]
    assert sorted(list(db.query_df(time_end=2, include_end=True).symbol)) == ["FB", "GOOG"]



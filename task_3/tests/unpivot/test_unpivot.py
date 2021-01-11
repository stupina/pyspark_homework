from pyspark import Row
from pyspark.sql import SparkSession

from task_3.unpivot.Unpivot import Unpivot


def test_unpivot_data_simple(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            (1, 'Ivan', 0.1, 0.1, 0.7),
            (2, 'Maria', 0.2, 0.5, 0.9)
        ],
        ['id', 'name', '10.02', '20.02', '28.02']
    )

    unpivot_transform = Unpivot(['id', 'name'], key_col='date', value_col='score')

    actual_df = unpivot_transform.unpivot(df)
    assert actual_df.columns == ['id', 'name', 'date', 'score']
    assert set(actual_df.collect()) == {Row(id=1, name='Ivan', date='20.02', score=0.1),
                                        Row(id=1, name='Ivan', date='10.02', score=0.1),
                                        Row(id=1, name='Ivan', date='28.02', score=0.7),
                                        Row(id=2, name='Maria', date='20.02', score=0.5),
                                        Row(id=2, name='Maria', date='10.02', score=0.2),
                                        Row(id=2, name='Maria', date='28.02', score=0.9)}


def test_unpivot_data_more_cols(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            (1, 'Ivan', 0.1, 0.1, 0.7, 0.98),
            (2, 'Maria', 0.2, 0.5, 0.9, 1.0)
        ],
        ['id', 'name', '10.02', '20.02', '28.02', '10.03']
    )

    unpivot_transform = Unpivot(['id', 'name'], key_col='date', value_col='score')

    actual_df = unpivot_transform.unpivot(df)
    assert actual_df.columns == ['id', 'name', 'date', 'score']
    assert set(actual_df.collect()) == {Row(id=1, name='Ivan', date='20.02', score=0.1),
                                        Row(id=1, name='Ivan', date='10.02', score=0.1),
                                        Row(id=1, name='Ivan', date='28.02', score=0.7),
                                        Row(id=1, name='Ivan', date='10.03', score=0.98),
                                        Row(id=2, name='Maria', date='20.02', score=0.5),
                                        Row(id=2, name='Maria', date='10.02', score=0.2),
                                        Row(id=2, name='Maria', date='28.02', score=0.9),
                                        Row(id=2, name='Maria', date='10.03', score=1.0)}


def test_unpivot_data_no_dynamic_cols(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            (1, 'Ivan'),
            (2, 'Maria')
        ],
        ['id', 'name']
    )

    unpivot_transform = Unpivot(['id', 'name'])

    actual_df = unpivot_transform.unpivot(df)
    assert actual_df.columns == ['id', 'name']
    assert actual_df.collect() == [Row(id=1, name='Ivan'),
                                   Row(id=2, name='Maria')]


def test_unpivot_data_no_constant_cols(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            (0.1, 0.1, 0.7, 0.98),
            (0.2, 0.5, 0.9, 1.0)
        ],
        ['10.02', '20.02', '28.02', '10.03']
    )

    unpivot_transform = Unpivot([], key_col='date', value_col='score')

    actual_df = unpivot_transform.unpivot(df)
    assert actual_df.columns == ['date', 'score']
    assert set(actual_df.collect()) == {Row(date='20.02', score=0.1), Row(date='10.02', score=0.1),
                                        Row(date='28.02', score=0.7), Row(date='10.03', score=0.98),
                                        Row(date='20.02', score=0.5), Row(date='10.02', score=0.2),
                                        Row(date='28.02', score=0.9), Row(date='10.03', score=1.0)}

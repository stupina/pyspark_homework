from pyspark import Row
from pyspark.sql import SparkSession

from task_3.history_product.HistoryProduct import HistoryProduct


def test_history_product_one_key(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.5),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', 0.8),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.8),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct(['id'])

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=0, name='Ivan', score=0.8, meta='changed'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='changed'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]


def test_history_product_one_key_old_nulls(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (0, 'Ivan', None),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', None),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.8),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct(['id'])

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=0, name='Ivan', score=0.8, meta='changed'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='changed'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]


def test_history_product_one_key_nulls(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (None, 'Ivan', None),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', None),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (None, 'Ivan', 0.8),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct(['id'])

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=None, name='Ivan', score=0.8, meta='changed'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='changed'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]


def test_history_product_one_key_new_nulls(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.5),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', 0.8),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (0, 'Ivan', None),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', None)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct(['id'])

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=0, name='Ivan', score=None, meta='changed'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='changed'),
                                 Row(id=4, name='Lukas', score=None, meta='inserted')]


def test_history_product_two_keys(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.5),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', 0.8),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.8),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct(['id', 'name'])

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=0, name='Ivan', score=0.8, meta='changed'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Jan', score=0.8, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='inserted'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]


def test_history_product_no_key(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.5),
            (1, 'Maria', 0.6),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', 0.8),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (0, 'Ivan', 0.8),
            (1, 'Maria', 0.6),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct()

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=0, name='Ivan', score=0.5, meta='deleted'),
                                 Row(id=0, name='Ivan', score=0.8, meta='inserted'),
                                 Row(id=1, name='Maria', score=0.6, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Jan', score=0.8, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='inserted'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]


def test_history_product_no_key_nulls(spark_session: SparkSession):
    df_old = spark_session.createDataFrame(
        [
            (None, 'Ivan', 0.5),
            (None, 'Maria', None),
            (2, 'Evgenii', 0.4),
            (3, 'Jan', 0.8),
        ],
        ['id', 'name', 'score']
    )

    df_new = spark_session.createDataFrame(
        [
            (None, 'Ivan', 0.8),
            (None, 'Maria', None),
            (3, 'Janus', 0.8),
            (4, 'Lukas', 0.7)
        ],
        ['id', 'name', 'score']
    )

    history_product = HistoryProduct()

    product = history_product.get_history_product(df_old, df_new)
    assert product.collect() == [Row(id=None, name='Ivan', score=0.5, meta='deleted'),
                                 Row(id=None, name='Ivan', score=0.8, meta='inserted'),
                                 Row(id=None, name='Maria', score=None, meta='not_changed'),
                                 Row(id=2, name='Evgenii', score=0.4, meta='deleted'),
                                 Row(id=3, name='Jan', score=0.8, meta='deleted'),
                                 Row(id=3, name='Janus', score=0.8, meta='inserted'),
                                 Row(id=4, name='Lukas', score=0.7, meta='inserted')]

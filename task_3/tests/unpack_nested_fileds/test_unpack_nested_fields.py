from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, Row

from task_3.unpack_nested_fields.unpack_nested_fields import UnpackNestedFields


def test_should_unpack_array(spark_session: SparkSession):
    df_1 = spark_session.createDataFrame([
        (1, "a", [1, 2, 3]),
        (2, "b", [4, 5, 6]),
    ], ["id", "text", "int_array"])

    unpacker = UnpackNestedFields()

    df_actual = unpacker.unpack_nested(df_1)

    fields_actual = [(field.name, field.dataType.typeName()) for field in df_actual.schema.fields]
    assert fields_actual == [
        ("id", LongType.typeName()),
        ("text", StringType.typeName()),
        ("int_array", LongType.typeName()),
    ]

    assert df_actual.collect() == [
        (1, "a", 1),
        (1, "a", 2),
        (1, "a", 3),
        (2, "b", 4),
        (2, "b", 5),
        (2, "b", 6),
    ]


def test_should_unpack_struct(spark_session: SparkSession):
    df_1 = spark_session.createDataFrame([
        (1, "a", Row(1, 'c', 3)),
        (2, "b", Row(4, 'd', 6)),
    ], ["id", "text", "struct"])

    unpacker = UnpackNestedFields()

    df_actual = unpacker.unpack_nested(df_1)

    fields_actual = [(field.name, field.dataType.typeName()) for field in df_actual.schema.fields]
    assert fields_actual == [
        ("id", LongType.typeName()),
        ("text", StringType.typeName()),
        ("struct__1", LongType.typeName()),
        ("struct__2", StringType.typeName()),
        ("struct__3", LongType.typeName()),
    ]

    assert df_actual.collect() == [
        (1, "a", 1, 'c', 3),
        (2, "b", 4, 'd', 6),
    ]


def test_should_unpack_struct_with_array(spark_session: SparkSession):
    df_1 = spark_session.createDataFrame([
        (1, "a", Row(1, 'c', [2, 3])),
        (2, "b", Row(4, 'd', [5, 6])),
    ], ["id", "text", "struct"])

    unpacker = UnpackNestedFields()

    df_actual = unpacker.unpack_nested(df_1)

    fields_actual = [(field.name, field.dataType.typeName()) for field in df_actual.schema.fields]
    assert fields_actual == [
        ("id", LongType.typeName()),
        ("text", StringType.typeName()),
        ("struct__1", LongType.typeName()),
        ("struct__2", StringType.typeName()),
        ("struct__3", LongType.typeName()),
    ]

    assert df_actual.collect() == [
        (1, "a", 1, 'c', 2),
        (1, "a", 1, 'c', 3),
        (2, "b", 4, 'd', 5),
        (2, "b", 4, 'd', 6),
    ]


def test_should_unpack_array_of_structs(spark_session: SparkSession):
    df_1 = spark_session.createDataFrame([
        (1, "a", [Row(1, 'c', 3), Row(2, 'e', 5)]),
        (2, "b", [Row(4, 'd', 6), Row(7, 'f', 8)]),
    ], ["id", "text", "struct"])

    unpacker = UnpackNestedFields()

    df_actual = unpacker.unpack_nested(df_1)

    fields_actual = [(field.name, field.dataType.typeName()) for field in df_actual.schema.fields]
    assert fields_actual == [
        ("id", LongType.typeName()),
        ("text", StringType.typeName()),
        ("struct__1", LongType.typeName()),
        ("struct__2", StringType.typeName()),
        ("struct__3", LongType.typeName()),
    ]

    assert df_actual.collect() == [
        (1, "a", 1, 'c', 3),
        (1, "a", 2, 'e', 5),
        (2, "b", 4, 'd', 6),
        (2, "b", 7, 'f', 8),
    ]


def test_should_not_unpack(spark_session: SparkSession):
    df_1 = spark_session.createDataFrame([
        (1, "a", "text"),
        (2, "b", "other_text"),
    ], ["id", "text", "other_text"])

    unpacker = UnpackNestedFields()

    df_actual = unpacker.unpack_nested(df_1)

    assert df_actual is df_1
    fields_actual = [(field.name, field.dataType.typeName()) for field in df_actual.schema.fields]
    assert fields_actual == [
        ("id", LongType.typeName()),
        ("text", StringType.typeName()),
        ("other_text", StringType.typeName()),
    ]

    assert df_actual.collect() == [
        (1, "a", "text"),
        (2, "b", "other_text"),
    ]

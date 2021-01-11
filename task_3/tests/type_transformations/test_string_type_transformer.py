import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, TimestampType, IntegerType, \
    BooleanType, LongType, DoubleType, DecimalType, Row

from task_3.type_transformations.string_type_transformer import StringTypeTransformer

PI_STRING = "3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825" \
            "342117067982148086513282306647093844609550582231725359408128481117450284102701938521105559" \
            "644622948954930381964428810975665933446128475648233786783165271201909145648566923460348610" \
            "454326648213393607260249141273724587006606315588174881520920962829254091715364367892590360" \
            "011330530548820466521384146951941511609433057270365759591953092186117381932611793105118548" \
            "074462379962749567351885752724891227938183011949129833673362440656643086021394946395224737" \
            "190702179860943702770539217176293176752384674818467669405132000568127145263560827785771342" \
            "757789609173637178721468440901224953430146549585371050792279689258923542019956112129021960" \
            "864034418159813629774771309960518707211349999998372978049951059731732816096318595024459455" \
            "346908302642522308253344685035261931188171010003137838752886587533208381420617177669147303" \
            "598253490428755468731159562863882353787593751957781857780532171226806613001927876611195909" \
            "2164201989"


def test_convert_strings_to_types(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            ("1", [1, 2, 3], '19-02-2020', '19-02-2020 00:00:00', "true", "1", "0.5", "123534627458685341",
             "123534627458685341"),
            ("2", [1, 2, 3], '19-02-2020', '19-02-2020 00:00:00', "false", "-1", "0.7891412462571346431",
             "234735684679046827457", "234735684679046827457"),
            ("3", [1, 2, 3], '19-02-2020', '19-02-2020 00:00:00', "True", "0", PI_STRING, PI_STRING, PI_STRING),
            ("4", [1, 2, 3], '19-02-2020', '19-02-2020 00:00:00', "not_true", "2147483648", "42",
             "-143763583573461346.0368479672", "-143763583573461346.0368479672"),
            ("5", [1, 2, 3], None, None, None, None, None, None, None),
        ],
        ["id", "array", "date", "timestamp", "boolean", "integer", "double", "decimal(38,0)", "decimal(24,5)"]
    )

    transformer = StringTypeTransformer()

    expected_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("array", ArrayType(LongType()), True),
            StructField("date", DateType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("boolean", BooleanType(), nullable=False),
            StructField("integer", IntegerType(), True),
            StructField("double", DoubleType(), True),
            StructField("decimal(38,0)", DecimalType(38, 0), True),
            StructField("decimal(24,5)", DecimalType(24, 5), True)
        ]
    )
    transformed = transformer.transform_dataframe(df, expected_schema)
    for i in transformed.schema:
        print('field', i)
    assert transformed.schema == expected_schema
    assert transformed.toJSON().collect() == [
        '{"id":"1","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":true,'
        '"integer":1,"double":0.5,"decimal(38,0)":123534627458685341,"decimal(24,5)":123534627458685341.00000}',
        '{"id":"2","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":false,'
        '"integer":-1,"double":0.7891412462571347,"decimal(38,0)":234735684679046827457}',
        '{"id":"3","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":true,'
        '"integer":0,"double":3.141592653589793,"decimal(38,0)":3,"decimal(24,5)":3.14159}',
        '{"id":"4","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":false,'
        '"double":42.0,"decimal(38,0)":-143763583573461346,"decimal(24,5)":-143763583573461346.03685}',
        '{"id":"5","array":[1,2,3],"boolean":false}']

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_date, to_timestamp
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName(
    'typesSession',
).getOrCreate()


class StringTypeTransformer(object):
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(
        self,
        dataframe: DataFrame,
        expected_schema: StructType,
    ):
        dataframe.show()
        dataframe.printSchema()

        for field in expected_schema.fields:
            if field.dataType.simpleString() == 'date':
                dataframe = dataframe.withColumn(
                    field.name,
                    to_date(
                        dataframe[field.name],
                        'dd-MM-yyyy',
                    ).cast(field.dataType),
                )
            elif field.dataType.simpleString() == 'timestamp':
                dataframe = dataframe.withColumn(
                    field.name,
                    to_timestamp(
                        dataframe[field.name],
                        'dd-MM-yyyy HH:mm:ss',
                    ).cast(field.dataType),
                )
            else:
                dataframe = dataframe.withColumn(
                    field.name,
                    dataframe[field.name].cast(field.dataType),
                )
            if not field.nullable:
                dataframe = dataframe.fillna({field.name: False})

        return dataframe

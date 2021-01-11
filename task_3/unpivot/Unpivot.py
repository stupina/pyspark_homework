from typing import List

from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName(
    "unpivotSession",
).getOrCreate()


class Unpivot(object):
    """
    Class provides unpivoting of some columns in dataset.
    For example for next dataset:
    +---+-----+-----+-----+-----+
    | id| name|10.02|20.02|28.02|
    +---+-----+-----+-----+-----+
    |  1| Ivan|  0.1|  0.1|  0.7|
    |  2|Maria|  0.2|  0.5|  0.9|
    +---+-----+-----+-----+-----+

    if we will consider `id` and `name` as constant columns, and columns 10.02, 20.02, 28.02 as dates,
    and other values as score it should provide next result:

    +---+-----+-----+-----+
    | id| name| date|score|
    +---+-----+-----+-----+
    |  1| Ivan|10.02|  0.1|
    |  1| Ivan|28.02|  0.7|
    |  1| Ivan|20.02|  0.1|
    |  2|Maria|10.02|  0.2|
    |  2|Maria|28.02|  0.9|
    |  2|Maria|20.02|  0.5|
    +---+-----+-----+-----+

    See spark sql function `stack`.
    """

    def __init__(self, constant_columns: List[str], key_col='', value_col=''):
        self.constant_columns = constant_columns
        self.key_col = key_col
        self.value_col = value_col

    def unpivot(self, dataframe: DataFrame) -> DataFrame:
        stack_len = len(dataframe.columns) - len(self.constant_columns)
        variative_columns = list(
            set(dataframe.columns).difference(set(self.constant_columns))
        )

        temp_columns = [
            f'temp{number}' for number, _ in enumerate(variative_columns)
        ]

        column_temp = zip(variative_columns, temp_columns)

        cnames_list = []
        for column, temp in column_temp:
            dataframe = dataframe.withColumnRenamed(column, temp)
            cnames_list.append(f"'{column}', {temp}")

        dataframe.createOrReplaceTempView('dataframe')

        stack_str = " stack({}, {}) as ({}, {})".format(
            stack_len,
            ', '.join(cnames_list),
            self.key_col,
            self.value_col,
        )
        query = "SELECT {}{}{} FROM dataframe".format(
            ', '.join(self.constant_columns) or '',
            self.constant_columns and cnames_list and ',' or '',
            cnames_list and stack_str or '',
        )

        return spark.sql(query)

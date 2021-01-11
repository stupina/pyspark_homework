from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, udf


def remove_az(value, chars, replacement):
    for char in chars:
        value = value.replace(char, replacement)
    return value


class IllegalCharRemover(object):
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError
        self.chars = ''.join(chars)
        self.replacement = replacement

    def remove_illegal_chars(
        self,
        dataframe: DataFrame,
        source_column: str,
        target_column: str,
    ):
        remove_az_udf = udf(remove_az)

        dataframe = dataframe.withColumn(
            target_column,
            remove_az_udf(
                dataframe[source_column],
                lit(self.chars),
                lit(self.replacement),
            ),
        )
        return dataframe.drop(source_column)

from pyspark.sql import DataFrame, Row, SparkSession


spark = SparkSession.builder.appName(
    "hpSession",
).getOrCreate()


def add_metadata(row):
    meta = 'not_changed'
    data = row.asDict()

    old = []
    new = []
    names_values = {}
    new_data = {}

    for column, value in data.items():
        if column.startswith('old_'):
            old.append(value is None)
        if column.startswith('new_'):
            new.append(value is None)

        names_values.setdefault(column[4:], []).append(value)

    if all(old) and not all(new):
        meta = 'inserted'
    elif all(new) and not all(old):
        meta = 'deleted'

    for name, value in names_values.items():
        old_value, new_value = value
        if meta == 'not_changed' and old_value != new_value:
            meta = 'changed'

        if meta == 'deleted':
            value = old_value
        else:
            value = new_value

        new_data.update({
            name: value,
        })

    new_data.update({
        'meta': meta,
    })

    return Row(**new_data)


class HistoryProduct(object):
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """
    def __init__(self, primary_keys=None):
        self.primary_keys = primary_keys

    def get_history_product(
        self,
        old_dataframe: DataFrame,
        new_dataframe: DataFrame,
    ):
        if not self.primary_keys:
            self.primary_keys = [
                field.name for field in old_dataframe.schema.fields
            ]

        if self.primary_keys:
            cond = [
                old_dataframe[key].eqNullSafe(new_dataframe[key])
                for key in self.primary_keys
            ]

            old_fields = [
                old_dataframe[field.name].alias(f'old_{field.name}')
                for field in old_dataframe.schema.fields
            ]
            new_fields = [
                new_dataframe[field.name].alias(f'new_{field.name}')
                for field in new_dataframe.schema.fields
            ]

            dataframe = old_dataframe.join(
                new_dataframe,
                cond,
                'full',
            ).select(
                *old_fields,
                *new_fields,
            )

            rdd = dataframe.rdd.map(
                add_metadata,
            )
            dataframe = spark.createDataFrame(rdd)

            dataframe.show()
        else:
            dataframe = new_dataframe

        dataframe.show()
        return dataframe

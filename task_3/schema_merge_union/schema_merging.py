from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def set_null_collumns_from_other_df(df1_names, df1, df2):
    for name in df1_names:
        field = next(filter(
            lambda field: field.name == name,
            df1.schema.fields,
        ))

        if field:
            df2 = df2.withColumn(
                field.name,
                lit(None).cast(field.dataType),
            )

    return df2


def add_intersections(names_intersection, df1, df2):
    for name in names_intersection:
        field_df1 = next(filter(
            lambda field: field.name == name,
            df1.schema.fields,
        ))

        field_df2 = next(filter(
            lambda field: field.name == name,
            df2.schema.fields,
        ))

        if field_df1.dataType != field_df2.dataType:
            field_df1_name = f'{name}_{field_df1.dataType.simpleString()}'
            df1 = df1.withColumnRenamed(name, field_df1_name)
            df2 = df2.withColumn(
                field_df1_name,
                lit(None).cast(field_df1.dataType),
            )

            field_df2_name = f'{name}_{field_df2.dataType.simpleString()}'
            df2 = df2.withColumnRenamed(name, field_df2_name)
            df1 = df1.withColumn(
                field_df2_name,
                lit(None).cast(field_df2.dataType),
            )

    return (df1, df2)


def reorder_dfs(df1, df2):
    df2 = df2.select(*df1.schema.fieldNames())
    return (df1, df2)


class SchemaMerging(object):
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        dataframe1_names = set(dataframe1.schema.fieldNames())
        dataframe2_names = set(dataframe2.schema.fieldNames())
        names_intersection = dataframe1_names.intersection(dataframe2_names)

        dataframe1_only_names = dataframe1_names.difference(dataframe2_names)
        dataframe2_only_names = dataframe2_names.difference(dataframe1_names)

        df2 = set_null_collumns_from_other_df(
            dataframe1_only_names,
            dataframe1,
            dataframe2,
        )

        df1 = set_null_collumns_from_other_df(
            dataframe2_only_names,
            dataframe2,
            dataframe1,
        )

        df1, df2 = add_intersections(names_intersection, df1, df2)

        df1, df2 = reorder_dfs(df1, df2)

        return df1.union(df2)

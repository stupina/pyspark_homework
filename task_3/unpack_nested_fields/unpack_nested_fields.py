from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode


def is_struct(dtype):
    return dtype.startswith("struct")


def is_array(dtype):
    return dtype.startswith("array")


def unpuck(dataframe):
    for field in dataframe.schema.fields:
        ftype = field.dataType.simpleString()
        if is_array(ftype):
            dataframe = dataframe.withColumn(
                field.name,
                explode(dataframe[field.name])
            )
        elif is_struct(ftype):
            main_names = [ffield.name for ffield in dataframe.schema.fields]
            additional_names = [
                col(f'{field.name}.{ffield.name}').alias(
                    f'{field.name}_{ffield.name}',
                )
                for ffield in field.dataType.fields
            ]
            dataframe = dataframe.select(
                *main_names,
                *additional_names,
            ).drop(field.name)

    nested_fields = []
    for ffield in dataframe.schema.fields:
        ftype = ffield.dataType.simpleString()
        is_nested = is_array(ftype) or is_struct(ftype)
        nested_fields.append(is_nested)

    if any(nested_fields):
        dataframe = unpuck(dataframe)

    return dataframe


class UnpackNestedFields(object):
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """
    def unpack_nested(self, dataframe: DataFrame):
        dataframe = unpuck(dataframe)
        dataframe.show()
        return dataframe

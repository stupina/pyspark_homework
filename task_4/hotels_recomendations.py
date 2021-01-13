import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    lead,
    max,
    round,
    to_timestamp,
    trim,
)
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName(
    'HotelsRecomendations',
).getOrCreate()


def get_paths_schemas(directory):
    """
    Gets paths and schemas for files: bids.txt, motels.txt, exchange_rate.txt
    """
    bids_header = (
        'MotelID',
        'BidDate',
        'HU',
        'UK',
        'NL',
        'US',
        'MX',
        'AU',
        'CA',
        'CN',
        'KR',
        'BE',
        'I',
        'JP',
        'IN',
        'HN',
        'GY',
        'DE',
    )
    bids_fields = [
        StructField(column, StringType()) for column in bids_header[:3]
    ]
    bids_floats = [
        StructField(column, FloatType()) for column in bids_header[3:]
    ]
    bids_fields.extend(bids_floats)
    bids_schema = StructType(bids_fields)

    rate_header = ('ValidFrom', 'CurrencyName', 'CurrencyCode', 'ExchangeRate')
    rate_fields = [
        StructField(column, StringType()) for column in rate_header[:-1]
    ]
    rate_fields.append(
        StructField(rate_header[-1], FloatType()),
    )
    rate_schema = StructType(rate_fields)

    motels_header = ('MotelID', 'MotelName', 'Country', 'URL', 'Comment')
    motels_schema = StructType([
        StructField(column, StringType()) for column in motels_header
    ])

    return {
        f'{directory}/input/bids.txt': bids_schema,
        f'{directory}/input/exchange_rate.txt': rate_schema,
        f'{directory}/input/motels.txt': motels_schema,
    }


def read_files(paths_schemas):
    """
    Reads files bids.txt, motels.txt, exchange_rate.txt as dataframes
    """
    dataframes = []
    for path, schema in paths_schemas.items():
        df = spark.read.csv(
            path,
            header=False,
            schema=schema,
        )
        dataframes.append(df)
    return dataframes


def str2date(dataframe, column):
    """
    Transforms string column to date
    """
    return dataframe.withColumn(
        column,
        to_timestamp(
            dataframe[column],
            'HH-dd-MM-yyyy',
        ).cast(TimestampType()),
    )


def date2str(dataframe, column):
    """
    Transforms date column to string
    """
    return dataframe.withColumn(
        column,
        date_format(
            dataframe[column],
            'yyyy-MM-dd HH:mm',
        ).cast(StringType()),
    )


def apply_currency_rate(bids):
    """
    Applies currency rate
    """
    return bids.withColumn(
        'Rate_EUR',
        round(col('Rate') / col('ExchangeRate'), 3),
    )


class HotelsRecomendations(object):
    """
    Class provides methods for Motels.home to find maximum prices values
    """

    def __init__(self, paths_schemas=None):
        self.directory = os.path.dirname(os.path.abspath(__file__))
        paths_schemas = get_paths_schemas(self.directory)
        bids, rate, motels = read_files(paths_schemas)
        self.bids = str2date(bids, 'BidDate')
        self.rate = str2date(rate, 'ValidFrom')
        self.motels = motels

    def filter_and_count_erros_in_bids(self):
        """
        Filters and count errors. Returns filtered bids and file with errors
        """
        bids = self.bids
        err_column = 'HU'
        date_column = 'BidDate'
        error_filter = bids[err_column].startswith('ERROR')

        errors = bids.filter(error_filter).select(
            date_column,
            trim(col(err_column)).alias('error'),
        ).groupBy(
            date_column,
            'error',
        ).count()

        errors.coalesce(1).write.csv(
            path=f'{self.directory}/output/errors',
            mode='overwrite',
            header=False,
        )

        self.bids = bids.filter(~error_filter).withColumn(
            err_column,
            bids[err_column].cast(FloatType()),
        )

    def set_bids_with_eur_rate(self):
        """
        Adds fitted EUR rates to bid rows
        """
        rate_with_next_dates = self.rate.withColumn(
            'ValidTo',
            lead('ValidFrom').over(
                Window.orderBy('ValidFrom'),
            ),
        )

        self.bids = self.bids.join(
            rate_with_next_dates,
            [
                self.bids['BidDate'] >= rate_with_next_dates['ValidFrom'],
                self.bids['BidDate'] < rate_with_next_dates['ValidTo'],
            ],
            'inner',
        ).select(
            *self.bids.schema.names,
            'ExchangeRate',
        )

    def set_bids_with_important_losa(self):
        """
        Keeps bids only with important locations of sale: US, MX, CA.

        Also:
         * Convert USD to EUR.
           The result should be rounded to 3 decimal precision.
         * Convert dates to proper format - "yyyy-MM-dd HH:mm"
           instead of original "HH-dd-MM-yyyy"
         * get rid of records where there is no price for a Losa
           or the price is not a proper decimal number
        """
        self.bids.createOrReplaceTempView('bids')
        important_losa = (
            'US',
            'MX',
            'CA',
        )
        important_losa_stack = [
            f"'{name}', {name}"
            for name in important_losa
        ]
        stack_str ="stack({}, {}) as ({}, {})".format(
            len(important_losa),
            ', '.join(important_losa_stack),
            'Losa',
            'Rate',
        )
        main_columns = ('MotelID', 'BidDate', 'ExchangeRate')
        query = "SELECT {}, {} FROM bids".format(
            ', '.join(main_columns),
            stack_str,
        )
        bids_with_important_losa = spark.sql(query)
        result_bids = apply_currency_rate(bids_with_important_losa)
        self.bids = result_bids.filter(result_bids['Rate'].isNotNull())
        self.bids = date2str(self.bids, 'BidDate')

    def set_bids_with_motels(self):
        """
        Enriches bids with motels names
        """
        self.bids = self.bids.join(self.motels, 'MotelID').select(
            *self.bids.schema.names,
            'MotelName',
        )

    def set_max_bids(self):
        """
        Finds maximum prices values
        """
        bids_max = self.bids.groupBy('MotelID', 'BidDate').agg(
            max('Rate').alias('Rate'),
        )

        self.bids = self.bids.join(
            bids_max,
            ['MotelID', 'BidDate', 'Rate'],
            'inner',
        )

    def prepare_data(self):
        """
        Prepares data for show
        """
        self.filter_and_count_erros_in_bids()
        self.set_bids_with_eur_rate()
        self.set_bids_with_important_losa()
        self.set_bids_with_motels()
        self.set_max_bids()

    def show(self):
        self.bids.show()


def main():
    hr = HotelsRecomendations()
    hr.prepare_data()
    hr.show()


if __name__ == '__main__':
    main()

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    'PySparkCountries',
).getOrCreate()


def main():
    dir = os.path.dirname(os.path.abspath(__file__))
    filepath = f'{dir}/countries.csv'

    df = spark.read.csv(
        filepath,
        header=True,
        inferSchema=True,
    )

    print('-'*20, '\n', 'Count:', df.count(), '\n', '-'*20)


if __name__ == '__main__':
    main()

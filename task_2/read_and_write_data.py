import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    'ReadWriteData',
).getOrCreate()


def main():
    dir = os.path.dirname(os.path.abspath(__file__))
    filepath = f'{dir}/microdados_enem_2016_coma.csv'

    df = spark.read.csv(
        filepath,
        header=True,
        inferSchema=True,
    )

    df.select(
        'NU_INSCRICAO',
        F.concat_ws(' | ','NU_ANO','NO_MUNICIPIO_RESIDENCIA'),
    ).rdd.map(
        lambda rec:(rec[0], rec[1])
    ).saveAsSequenceFile(f'{dir}/output')
    seq_df = spark.sparkContext.sequenceFile(f'{dir}/output').toDF()
    print(seq_df.show(5))

    df.write.format('avro').save(f'{dir}/enem.avro')

    df.write.format('orc').save(f'{dir}/enem.orc')

    df.select(
        'NU_INSCRICAO',
        'NU_ANO',
    ).write.option(
        'compression',
        'gzip',
    ).parquet(
        path=f'{dir}/enem_gzip.parquet',
        mode='overwrite',
    )

    df.select(
        'NU_INSCRICAO',
        'NU_ANO',
    ).write.option(
        'compression',
        'lzo',
    ).parquet(
        path=f'{dir}/enem_lzo.parquet',
        mode='overwrite',
    )

    df.select(
        'NU_INSCRICAO',
        'NU_ANO',
    ).write.option(
        'compression',
        'snappy',
    ).parquet(
        path=f'{dir}/enem_snappy.parquet',
        mode='overwrite',
    )

    df.select(
        'NU_INSCRICAO',
        'NU_ANO',
    ).write.option(
        'compression',
        'uncompressed',
    ).parquet(
        path=f'{dir}/enem.parquet',
        mode='overwrite',
    )

    df.select(
        'NU_INSCRICAO',
        'NU_ANO',
    ).write.option(
        'compression',
        'lz4',
    ).parquet(
        path=f'{dir}/enem_lz4.parquet',
        mode='overwrite',
    )


if __name__ == '__main__':
    main()

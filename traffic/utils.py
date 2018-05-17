""":mod:`utils`
"""
import pyspark
import pyspark.sql.functions


def load_raw_counts(file_path, delimiter=' '):
    """Reads in the contents of CSV file defined by *file_path*.

    Limited to two-column, separated by *delimiter* data arrangement with
    :class:`pyspark.sql.types.TimestampType` and
    :class:`pyspark.sql.types.IntegerType` types.

    Returns:
        DataFrame representing the sample data set

    """
    _sc = pyspark.SparkContext.getOrCreate()
    sql = pyspark.sql.SQLContext(_sc)

    schema = pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('sample_date', pyspark.sql.types.TimestampType(), True),
        pyspark.sql.types.StructField('count', pyspark.sql.types.IntegerType(), True)
    ])

    return sql.read.csv(file_path,
                        header=False,
                        sep=delimiter,
                        timestampFormat="yyyy-MM-dd'T'HH:mm:ss",
                        schema=schema)


def aggregate(file_path):
    """Aggregate vehicle counts.

    Returns:
        integer representation of the sum of all vehicle counts

    """
    result = load_raw_counts(file_path).\
        agg(pyspark.sql.functions.sum('count').alias('all_vehicles'))
    result.show()

    return result.collect()[0].asDict().get('all_vehicles')


def daily_aggregate(file_path):
    """Aggregate vehicles by day.

    Returns:
        integer representation of the sum of all vehicle counts
        on a daily basis

    """
    result = load_raw_counts(file_path).\
        groupBy(pyspark.sql.functions.date_format('sample_date',
                                                  format='yyyy-MM-dd').alias('day')).\
        agg(pyspark.sql.functions.sum('count').alias('daily_count'))
    result.show()

    return [(x['day'], x['daily_count']) for x in result.collect()]


def sorted_counts(file_path, order='desc', limit=None, interval=30):
    """Sort vehicles counts ordered by *order*.

    If *limit* is set then that value is used to limit rows returned.

    The default *interval* is 30 minutes but this can be modified
    to group interval segments.

    Returns:
        list if counts order by sort criteria and interval

    """
    result = load_raw_counts(file_path)
    _col = pyspark.sql.functions.col('ordered_count')
    if order == 'desc':
        sorter = _col.desc
    else:
        sorter = _col.asc

    result = result.\
        groupBy(pyspark.sql.functions.window('sample_date',
                                             '{} minutes'.format(interval)).\
        alias('sample_date')).\
        agg(pyspark.sql.functions.sum('count').alias('ordered_count')).\
        orderBy(sorter())
    result.show(100, False)

    if limit is None:
        limit = -1

    if interval == 30:
        result = [(x['sample_date'][0], x['ordered_count']) for x in result.collect()][0:limit]
    else:
        result = [(x['sample_date'][0],
                   x['sample_date'][1],
                   x['ordered_count']) for x in result.collect()][0:limit]

    return result

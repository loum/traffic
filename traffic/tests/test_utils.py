""":class:`Traffic` unit test cases.

"""
import os
import pytest
import datetime
import pyspark.sql.dataframe

import traffic.utils

@pytest.mark.usefixtures('spark_context')
def test_read():
    """Read sample data
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I read the data
    received = traffic.utils.load_raw_counts(source_file)

    # then I should receive a DataFrame type
    msg = 'DataFrame instance not produced'
    assert isinstance(received, pyspark.sql.dataframe.DataFrame)


@pytest.mark.usefixtures('spark_context')
def test_aggregate_all():
    """Aggregate all traffic.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I aggregate the total number of vehicles
    received = traffic.utils.aggregate(source_file)

    # then I should match the total count
    msg = 'Total aggregate error'
    assert received == 398, msg


@pytest.mark.usefixtures('spark_context')
def test_daily_aggregate_all():
    """Daily aggregate all traffic.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I aggregate the total number of vehicles
    received = traffic.utils.daily_aggregate(source_file)

    # then I should match the total count
    expected = [
        ('2016-12-01', 179), 
        ('2016-12-09', 4), 
        ('2016-12-05', 81), 
        ('2016-12-08', 134)
    ]
    msg = 'Daily aggregate error'
    assert received == expected, msg


@pytest.mark.usefixtures('spark_context')
def test_sorted_counts_descending():
    """Sort traffic counts: descending.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I sort the vehicles counts
    received = traffic.utils.sorted_counts(source_file)

    # then I should match the total count
    expected = (datetime.datetime(2016, 12, 1, 7, 30), 46)
    msg = 'Daily aggregate error'
    assert received[0] == expected, msg


@pytest.mark.usefixtures('spark_context')
def test_sorted_counts_ascending():
    """Sort traffic counts: ascending.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I sort the vehicles counts
    received = traffic.utils.sorted_counts(source_file, order='asc')

    # then I should match the total count
    expected = (datetime.datetime(2016, 12, 1, 23, 30), 0)
    msg = 'Daily aggregate error'
    assert received[0] == expected, msg


@pytest.mark.usefixtures('spark_context')
def test_sorted_counts_descending_with_limit():
    """Sort traffic counts: ascending.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I sort the vehicles counts
    received = traffic.utils.sorted_counts(source_file, limit=3)

    # then I should match the total count
    expected = [
        (datetime.datetime(2016, 12, 1, 7, 30), 46),
        (datetime.datetime(2016, 12, 1, 8, 0), 42),
        (datetime.datetime(2016, 12, 8, 18, 0), 33)
    ]
    msg = 'Daily aggregate error'
    assert received == expected, msg


@pytest.mark.usefixtures('spark_context')
def test_sorted_counts_ascending_with_limit_and_interval():
    """Sort traffic counts: ascending.
    """
    # Given a source CSV file
    source_file = os.path.join('traffic',
                               'tests',
                               'files',
                               'sample.csv')

    # when I set the interval into 90 minute blocks
    interval = 90

    # and I sort the vehicles counts
    received = traffic.utils.sorted_counts(source_file,
                                           order='asc',
                                           limit=3,
                                           interval=90)

    # then I should match the total count
    expected = [
        (datetime.datetime(2016, 12, 1, 23, 0), datetime.datetime(2016, 12, 2, 0, 30), 0),
        (datetime.datetime(2016, 12, 5, 11, 0), datetime.datetime(2016, 12, 5, 12, 30), 7),
        (datetime.datetime(2016, 12, 1, 14, 0), datetime.datetime(2016, 12, 1, 15, 30), 9)
    ]
    msg = 'Ascending count over 90 minute interval error'
    assert received == expected, msg

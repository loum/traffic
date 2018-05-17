"""Global fixture arrangement at the `traffic` package level

"""
import pytest
import pyspark


@pytest.fixture(scope='session')
def spark_context(request):
    """Set up the spark context with appropriate config for test.
    """
    conf = pyspark.SparkConf()
    conf.set('spark.executor.memory', '1g')
    conf.set('spark.cores.max', '1')
    conf.set('spark.app.name', 'test')
    conf.set('spark.ui.port', '4050')
    _sc = pyspark.SparkContext(conf=conf)

    def fin():
        """Clean up.
        """
        _sc.stop()

    request.addfinalizer(fin)

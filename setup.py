"""Setup script for the Traffic (Seek coding challenge) project.
"""
import os
import setuptools

PROJECT_NAME = os.path.basename(os.path.abspath(os.curdir))

PROD_PACKAGES = [
    'logga>=1.0.2',
    'pyspark>=2.2.0',
]

DEV_PACKAGES = [
    'pylint',
    'pytest',
    'pytest-cov',
    'sphinx_rtd_theme',
    'twine',
    'Sphinx',
]

PACKAGES = list(PROD_PACKAGES)
if (os.environ.get('APP_ENV') is not None and
        'dev' in os.environ.get('APP_ENV')):
    PACKAGES += DEV_PACKAGES

SETUP_KWARGS = {
    'name': PROJECT_NAME,
    'version': '0.0.0',
    'description': 'Seek coding challenge',
    'author': 'Lou Markovski',
    'author_email': 'lou.markovski@gmail.com',
    'url': 'http://triple20.com.au',
    'install_requires': PACKAGES,
    'packages': setuptools.find_packages(),
    'package_data': {
        PROJECT_NAME: [],
    },
    'scripts': [],
    'license': 'MIT',
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
}

setuptools.setup(**SETUP_KWARGS)

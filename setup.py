import os

from setuptools import setup, find_packages

exec(open('hops/version.py').read())

def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname), encoding='utf8').read() #python3
    except:
        return open(os.path.join(os.path.dirname(__file__), fname)).read() #python2

setup(
    name='hopsworks-cloud-sdk',
    version=__version__,
    install_requires=[
        'numpy',
        'pandas',
        'pyhopshive[thrift]',
        'boto3>=1.9.226',
        'SQLAlchemy',
        'PyMySQL',
        'pyopenssl',
        'idna'
    ],
    extras_require={
        'docs': [
            'sphinx',
            'sphinx-autobuild',
            'recommonmark',
            'sphinx_rtd_theme',
            'jupyter_sphinx_theme'
        ],
        'test': [
            'mock',
            'pytest',
        ],
        'plotting': ['matplotlib', 'seaborn']
    },
    author='Steffen Grohsschmiedt',
    author_email='steffen@logicalclocks.com',
    description='An SDK to integrate cloud solutions such as SageMaker and Databricks with Hopsworks.',
    license='Apache License 2.0',
    keywords='Hopsworks, SageMaker, Databricks',
    url='https://github.com/logicalclocks/hopsworks-cloud-sdk',
    download_url='http://snurran.sics.se/hops/hopsworks-cloud-sdk/hops-' + __version__ + '.tar.gz',
    packages=find_packages(exclude=['tests']),
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ]
)

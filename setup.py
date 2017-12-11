from setuptools import setup

setup(
    name='graphouse-graphite-api',
    version='0.1.0',
    url='https://github.com/swoop-inc/graphouse-graphite-api',
    license='MIT',
    author='Mark Bell',
    author_email='mark@swoop.com',
    description='Graphouse storage adaptor for graphite-api',
    zip_safe=False,
    install_requires=[
        'requests',
        'six'
    ]
)
from setuptools import setup, find_packages

setup(
    name='graphouse-graphite-api',
    version='0.1.1',
    url='https://github.com/swoop-inc/graphouse-graphite-api',
    license='MIT',
    author='Mark Bell',
    author_email='mark@swoop.com',
    description='Graphouse storage adaptor for graphite-api',
    packages=find_packages('.'),
    zip_safe=False,
    install_requires=[
        'requests',
        'six'
    ]
)

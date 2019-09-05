from setuptools import setup, find_packages

setup(
    name='COGO Bike Share',
    version='0.0.0',
    packages=find_packages(),
    include_package_data=True,
    description='Packages helper functions to analyze COGO bike share usage',
    author='Chris Wetherill',
    author_email='chris@tbmh.org',
    license='MIT',
    test_suite='nose.collector',
    tests_require=['nose'],
    zip_safe=False)

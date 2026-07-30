from setuptools import find_packages, setup

setup(
    name='gfdl_utils',
    url='https://github.com/hdrake/gfdl_utils.git',
    version='0.2.0',
    description='Utilities for working with the GFDL filesystem from Python.',
    packages=find_packages(exclude=['tests', 'tests.*', 'notebooks']),
    python_requires='>=3.7',
    extras_require={'test': ['pytest']},
    license='MIT',
)

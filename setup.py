from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='dynamodb-lens',
    version='23.10.dev3',
    url='https://github.com/anorth848/dynamodb-lens',
    license='GNU General Public License v3.0',
    author='Adam North',
    author_email='anorth848@gmail.com',
    python_requires='>=3.7',
    description='DynamoDB Lens - Analyze your DynamoDB environment',
    install_requires=['boto3>=1.28.67'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(include=['dynamodb_lens', 'dynamodb_lens.*']),
    keywords='dynamodb dynamodb-lens'
)

from setuptools import setup, find_packages

setup(
    name="langchain-snowpoc",
    version="0.1.0",
    packages=find_packages(),
    description="This is a PoC for Snowflake integration with Langchain",
    author="Bart Wrobel",
    author_email="124384994+b-art-b@users.noreply.github.com",
    url="https://github.com/b-art-b/langchain-snowpoc",
    install_requires=[
        "langchain-community==0.3.1",
        "langchain-core==0.3.7",
        "langchain-text-splitters==0.3.0",
        "langchain==0.3.1",
        "langsmith==0.1.129",
    ],
)

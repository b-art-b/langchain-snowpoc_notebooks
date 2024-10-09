# langchain-snowpoc

> **NOTE**: This is just a PoC, not production-ready and covers just a few use cases.

This is a PoC of how can Cortex be used with Langchain.
The code was updated from the original code base to fit
new signatures of Snowflake functions and LangChain 0.3.

It shows how easy it is to integrate Langchain, Snowflake
Cortex and data in Stages.


## Setup

Make sure you use `make`, and that your Snowflake connection is configured.
Update the Makefile accordingly

Run:

```bash
make all
```


## Notebook

1. Create new Notebook in Snowflake by importing the `notebook_with_langchain.ipynb`
1. Add the libraries you can find in the `environment.yml` file



> **Note**: This repo is for Medium blog post: TBD


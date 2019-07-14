# Matching Workflow
A workflow to match new products with already inserted in a database
## Getting Started
### Installing
~~~
pip install -r requirements.txt
make install
~~~

## Running the tests
### Local
testing
~~~
python workflow.py CleanUnmatchedProductsFile --local-scheduler
~~~
delete targets
~~~
make clean
~~~


## Deployment
### Running
~~~
make run
~~~


# dada is data_about(data)
 
**dada** is an add-on package for [`dbt`](https://www.getdbt.com/) that helps
you, the heroic data analyst or engineer, rapidly explore and profile the data in 
your data warehouse.

## Supported databases

* PostgreSQL

Other databases may work as much of the code is standard SQL. 
Please contribute by testing dada on your database, and 
[raising an issue](https://github.com/dah33/dbt-dada/issues) 
if it doesn't work as expected.

## Inspiration

* R's `glimpse()` function
* Python's `pandas_profiling` module
* data-mie's `dbt_profiler` package
* csvkit's `csvstat` command
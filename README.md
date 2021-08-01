# dada is making DAta about DAta
 
`dada` is an add-on package for [`dbt`](https://www.getdbt.com/) that helps
you, the heroic data analyst or engineer, rapidly explore and profile the data in 
your data warehouse.

It can be used interactively for exploration, or on a schedule to track 
the evolution of data.

## Supported databases

* PostgreSQL

Other databases may work as much of the code is standard SQL. 
Please contribute by testing dada on your database, and 
[raising an issue](https://github.com/dah33/dbt-dada/issues) 
if it doesn't work as expected.

## Inspiration

* R's `summary()` and `glimpse()` functions
* Python's `pandas_profiling` module
* data-mie's `dbt_profiler` package
* csvkit's `csvstat` command
* Adam Aspin's article [Data Profiling with T-SQL](https://www.sqlservercentral.com/articles/data-profiling-with-t-sql)
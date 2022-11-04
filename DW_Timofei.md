# Data warehouse - OLAP Database
## PostreSQL with Citus extension
The Postgres with Citus extension is chosen as a data warehouse database.


### PostgreSQL
The [PostgreSQL](https://github.com/postgres/postgres) is open source object-relational database solution
with a solid background, up-to-date documentation, popularity across top-tech companies and strong 
community.

The database supports variety datatypes including json/jsonb, all the innovation come from Postgres extensions.
It has different optimization tools for analytical queries such as [indexes](https://www.tutorialspoint.com/postgresql/postgresql_indexes)
and [table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html).


### Citus
However, the build-in features delivers a powerful tool,
but for better analytical query processing and potential database growth the [Citus](https://github.com/citusdata/citus) extension would be also used in solution.
Citus is a postgres extension that support sharding, paralleling SQL across multiple nodes [supported and owned](https://www.citusdata.com/about/our-story/) by Microsoft. It delivers insanely fast performance. Even with real-time data ingest and billions of rows of data.
The idea of Citus extension is originally designed as OLAP solution based on PostgreSQL.
Moreover, the focus of development is changes and today Citus do either: OLTP and [HTAP](https://en.wikipedia.org/wiki/Hybrid_transactional/analytical_processing).

### Clickhouse benchmark (Citus vs MariaDB with ColumnStore)
According to clickhouse [benchmark](https://benchmark.clickhouse.com/#eyJzeXN0ZW0iOnsiQXRoZW5hIChwYXJ0aXRpb25lZCkiOmZhbHNlLCJBdGhlbmEgKHNpbmdsZSkiOmZhbHNlLCJBdXJvcmEgZm9yIE15U1FMIjpmYWxzZSwiQXVyb3JhIGZvciBQb3N0Z3JlU1FMIjpmYWxzZSwiQnl0ZUhvdXNlIjpmYWxzZSwiQ2l0dXMiOnRydWUsImNsaWNraG91c2UtbG9jYWwgKHBhcnRpdGlvbmVkKSI6ZmFsc2UsImNsaWNraG91c2UtbG9jYWwgKHNpbmdsZSkiOmZhbHNlLCJDbGlja0hvdXNlIjpmYWxzZSwiQ2xpY2tIb3VzZSAodHVuZWQpIjpmYWxzZSwiQ2xpY2tIb3VzZSAoenN0ZCkiOmZhbHNlLCJDbGlja0hvdXNlIENsb3VkIjpmYWxzZSwiQ3JhdGVEQiI6ZmFsc2UsIkRhdGFiZW5kIjpmYWxzZSwiZGF0YWZ1c2lvbiI6ZmFsc2UsIkRydWlkIjpmYWxzZSwiRHVja0RCIjpmYWxzZSwiRWxhc3RpY3NlYXJjaCI6ZmFsc2UsIkVsYXN0aWNzZWFyY2ggKHR1bmVkKSI6ZmFsc2UsIkdyZWVucGx1bSI6ZmFsc2UsIkhlYXZ5QUkiOmZhbHNlLCJJbmZvYnJpZ2h0IjpmYWxzZSwiTWFyaWFEQiBDb2x1bW5TdG9yZSI6dHJ1ZSwiTWFyaWFEQiI6dHJ1ZSwiTW9uZXREQiI6ZmFsc2UsIk1vbmdvREIiOmZhbHNlLCJNeVNRTCAoTXlJU0FNKSI6ZmFsc2UsIk15U1FMIjpmYWxzZSwiUGlub3QiOmZhbHNlLCJQb3N0Z3JlU1FMIjpmYWxzZSwiUXVlc3REQiAocGFydGl0aW9uZWQpIjpmYWxzZSwiUXVlc3REQiI6ZmFsc2UsIlJlZHNoaWZ0IjpmYWxzZSwiU2VsZWN0REIiOmZhbHNlLCJTaW5nbGVTdG9yZSI6ZmFsc2UsIlNub3dmbGFrZSI6ZmFsc2UsIlNRTGl0ZSI6ZmFsc2UsIlN0YXJSb2NrcyAodHVuZWQpIjpmYWxzZSwiU3RhclJvY2tzIjpmYWxzZSwiVGltZXNjYWxlREIgKGNvbXByZXNzaW9uKSI6ZmFsc2UsIlRpbWVzY2FsZURCIjpmYWxzZX0sInR5cGUiOnsic3RhdGVsZXNzIjp0cnVlLCJtYW5hZ2VkIjp0cnVlLCJKYXZhIjp0cnVlLCJjb2x1bW4tb3JpZW50ZWQiOnRydWUsIkMrKyI6dHJ1ZSwiTXlTUUwgY29tcGF0aWJsZSI6dHJ1ZSwicm93LW9yaWVudGVkIjp0cnVlLCJDIjp0cnVlLCJQb3N0Z3JlU1FMIGNvbXBhdGlibGUiOnRydWUsIkNsaWNrSG91c2UgZGVyaXZhdGl2ZSI6dHJ1ZSwiZW1iZWRkZWQiOnRydWUsIlJ1c3QiOnRydWUsInNlYXJjaCI6dHJ1ZSwiZG9jdW1lbnQiOnRydWUsInRpbWUtc2VyaWVzIjp0cnVlfSwibWFjaGluZSI6eyJzZXJ2ZXJsZXNzIjp0cnVlLCIxNmFjdSI6dHJ1ZSwiTCI6dHJ1ZSwiTSI6dHJ1ZSwiUyI6dHJ1ZSwiWFMiOnRydWUsImM2YS40eGxhcmdlLCA1MDBnYiBncDIiOnRydWUsImM1LjR4bGFyZ2UsIDUwMGdiIGdwMiI6dHJ1ZSwiYzZhLm1ldGFsLCA1MDBnYiBncDIiOnRydWUsIjE2IHRocmVhZHMiOnRydWUsIjIwIHRocmVhZHMiOnRydWUsIjI0IHRocmVhZHMiOnRydWUsIjI4IHRocmVhZHMiOnRydWUsIjMwIHRocmVhZHMiOnRydWUsIjQ4IHRocmVhZHMiOnRydWUsIm01ZC4yNHhsYXJnZSI6dHJ1ZSwiZjE2cyB2MiI6dHJ1ZSwiYzZhLjR4bGFyZ2UsIDE1MDBnYiBncDIiOnRydWUsInJhMy4xNnhsYXJnZSI6dHJ1ZSwicmEzLjR4bGFyZ2UiOnRydWUsInJhMy54bHBsdXMiOnRydWUsIlMyNCI6dHJ1ZSwiUzIiOnRydWUsIjJYTCI6dHJ1ZSwiM1hMIjp0cnVlLCI0WEwiOnRydWUsIlhMIjp0cnVlfSwiY2x1c3Rlcl9zaXplIjp7IjEiOnRydWUsIjIiOnRydWUsIjQiOnRydWUsIjgiOnRydWUsIjEyIjp0cnVlLCIxNiI6dHJ1ZSwiMzIiOnRydWUsIjY0Ijp0cnVlLCIxMjgiOnRydWUsInNlcnZlcmxlc3MiOnRydWUsInVuZGVmaW5lZCI6dHJ1ZX0sIm1ldHJpYyI6ImxvYWQiLCJxdWVyaWVzIjpbdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZV19), 
the main SQL competitor is MariaDB with ColumnStore extension. However, it des not support all SQL operations. The Citus outperforms the MariaDB in DB storage size and loading the data to the database,
but less performant in cold and hot data retrieving.
In the benchmark the indexes optimization were not used for Postgres solution, but with them, it would outperform the main competitor.

### Previous experience
Our team have previous experience with PostgreSQL database and can accept every challenge.

### Production implementation facts
[Postgres with Citus extension](https://techcommunity.microsoft.com/t5/azure-database-for-postgresql/architecting-petabyte-scale-analytics-by-scaling-out-postgres-on/ba-p/969685) is used on Azure for petabyte-scale analytical solution.
# coding challenge

## Assumptions
...

## How To

To setup the database and import the organisations run:

```
make setup
```

To import user events for a single data, e.g. '2020-12-06' run:
```
IMPORT_DATE='2020-12-06' make import
```
**Note**: as there is no event id that could be used as primary key running this multiple times will lead to duplicate rows.

To clean up afterwards (stop and remove database and import services) run:
```
make teardown
```

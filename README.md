# coding challenge

## Overview
The `Makefile` indicates what can be done and simplifies the interactions with the services. Each target can be run via
```
make <target>
```
The targets are:
* **setup:** start the database, rabbitmq and consumer listening for user events
* **import:** import
   * organizations via `COPY` command 
   * user events by emitting them to the queue
* **users_dim:** process user events to fill `crm.users_dim` (expects the environment variable `IMPORT_DATE` to be set)
* **user_events_daily:** process user events to fill `reports.user_events_daily` (expects the environment variable `IMPORT_DATE` to be set)
* **test_docker:** run unit tests
* **lint_docker:** run lint checker (including pylint, flake8, black and mypy)
* **e2e:** start a clean database, and test the etls from end to end
* **teardown:** stop services and remove the containers

## Running the ETLs
To process all data for all dates (`2020-12-05` - `2020-12-11`) run
```
make setup
```
for starting the services and
```
make import
```
for importing initial data.

**Note**: as there is no event id that could be used as primary key running this multiple times will lead to duplicate rows.

Finally run
```
days=( 05 06 07 08 09 10 11 )
for i in "${days[@]}"
do
	IMPORT_DATE=2020-12-$i make users_dim
	IMPORT_DATE=2020-12-$i make user_events_daily
done
```
Afterwards you will have two tables filled:
* **`crm.users_dim`:** a dimension table with historic user data
* **`reports.user_events_daily`:** a report with daily numbers
To setup the database and import the organizations run:

## Tests

### linting and formating
Check whether the code follows conventions defined via
* black
* mypy
* pylint
* flake8

by running:
```
make lint_docker
```

### Unit Tests
You can run unit tests by running
```
make test_docker
```
You will see a coverage report.

### Integration Tests
You can run an end2end test by running
```
make e2e
```
This will
* emit some events to the queue
* fill the tables `crm.users_dim` and `reports.user_events_daily`
* check if the tables contain what we expect


## Cleanup

To clean up afterwards (stop and remove database and import services) run:
```
make teardown
```

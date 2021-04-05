.PHONY:	setup import teardown

setup:
	docker-compose up -d postgres
	docker-compose build import
	docker-compose run --rm --entrypoint 'python /usr/src/import/etls/import_organizations.py' import

import:
	docker-compose run --rm import

users_dim:
	docker-compose run --rm --entrypoint 'python /usr/src/import/etls/users_dim.py' import

user_events_daily:
	docker-compose run --rm --entrypoint 'python /usr/src/import/etls/user_events_daily.py' import

test_docker:
	docker-compose run --rm --entrypoint 'make test' import

lint_docker:
	docker-compose run --rm --entrypoint 'make lint' import

e2e:
	make teardown
	make setup
	docker-compose run --rm --entrypoint 'make e2e' import
	make teardown

teardown:
	docker-compose down

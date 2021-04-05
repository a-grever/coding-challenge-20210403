.PHONY:	setup import teardown

setup:
	make teardown
	docker-compose build
	docker-compose up -d postgres
	docker-compose up -d queue
	docker-compose up -d consumer

import:
	docker-compose run --rm --entrypoint 'python /usr/src/warehouse/warehouse/import_organizations.py' worker
	docker-compose run --rm --entrypoint 'python /usr/src/warehouse/warehouse/user_event_producer.py' worker

users_dim:
	docker-compose run --rm --entrypoint 'python /usr/src/warehouse/warehouse/users_dim.py' worker

user_events_daily:
	docker-compose run --rm --entrypoint 'python /usr/src/warehouse/warehouse/user_events_daily.py' worker

test_docker:
	docker-compose run --rm --entrypoint 'make test' worker

lint_docker:
	docker-compose run --rm --entrypoint 'make lint' worker

e2e:
	make teardown
	make setup
	docker-compose run --rm --entrypoint 'make e2e' worker
	make teardown

teardown:
	docker-compose down

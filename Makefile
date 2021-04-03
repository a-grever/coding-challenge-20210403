.PHONY:	setup import teardown

setup:
	docker-compose up -d postgres
	docker-compose build import
	docker-compose run --rm --entrypoint 'python /usr/src/import/etls/import_organisations.py' import

import:
	docker-compose run --rm import

teardown:
	docker-compose down

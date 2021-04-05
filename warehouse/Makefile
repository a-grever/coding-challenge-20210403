.PHONY:	test lint

lint:
	black --check -t py38 -l 100 .
	pylint --rcfile=./pylint.cfg warehouse
	flake8
	mypy --ignore-missing-imports warehouse

test:
	coverage run --rcfile=setup.cfg --source=./warehouse -m pytest
	coverage report --rcfile=setup.cfg

e2e:
	pytest ./tests/e2e.py

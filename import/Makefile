.PHONY:	test lint

lint:
	black --check -t py38 -l 100 .
	pylint etls
	flake8
	mypy etls

test:
	coverage run --rcfile=setup.cfg --source=./etls -m pytest
	coverage report --rcfile=setup.cfg

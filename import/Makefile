.PHONY:	test lint

lint:
	black --check -t py38 -l 100 .
	pylint --rcfile=./pylint.cfg etls
	flake8
	mypy --ignore-missing-imports etls

test:
	coverage run --rcfile=setup.cfg --source=./etls -m pytest
	coverage report --rcfile=setup.cfg

e2e:
	pytest ./tests/e2e.py

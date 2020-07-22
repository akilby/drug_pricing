args=--update

setup:
	pip install pipenv
	pipenv install
	pipenv run python -m spacy download en_core_web_sm

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage
	rm -f .coverage.*

clean-slurm:
	rm -rf slurm*

clean: clean-pyc clean-test clean-slurm

run:
	pipenv run scheduler $(args)

test:
	pipenv run py.test tests --cov=src --cov-report=term-missing --cov-fail-under 50

lint:
	pipenv run flake8 src
	pipenv run mypy src

format:
	pipenv run black src

check: format lint test


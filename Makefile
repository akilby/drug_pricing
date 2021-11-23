args=--update

setup:
	python -m virtualenv .venv + 
	source .venv/bin/activate + 
	python -m pip install -r requirements.txt +
	python -m spacy download en_core_web_sm

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
	python -m src $(args)

test:
	python -m py.test tests --cov=src --cov-report=term-missing --cov-fail-under 50

lint:
	python -m flake8 src
	python -m mypy src

format: python -m black src

check: format lint test

build-html:
	pipenv run jupyter nbconvert --to html notebooks/summary.ipynb
	mv notebooks/summary.html index.html

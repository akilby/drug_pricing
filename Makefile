args=
base=python -m src.__init__

clean:
	rm -rf slurm* ~* *~ \#* *\#

update:
	pipenv run $(base) --update $(args)

histories:
	pipenv run $(base) --histories $(args)

spacy:
	pipenv run $(base) --spacy $(args)

run:
	pipenv run $(args)

test:
	pipenv run python -m unittest

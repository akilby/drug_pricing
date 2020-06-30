args="--update"

clean:
	rm -rf slurm* ~* *~ \#* *\#

run:
	pipenv run python -m src.scheduler $(args)

test: 
	pipenv run python -m unittest

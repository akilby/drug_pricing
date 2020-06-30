ARGS="--update"

clean:
	rm -rf slurm* ~* *~ \#* *\#

run:
	pipenv run python -m src.scheduler $(ARGS)

test: 
	pipenv run python -m unittest

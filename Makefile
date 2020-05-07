ARGS="--update"

clean:
	rm -rf slurm* ~* *~ \#* *\#

run:
	pipenv run python scheduler.py $(ARGS)

test: 
	pipenv run python -m unittest

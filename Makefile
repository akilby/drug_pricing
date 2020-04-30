ARGS="--update"

clean:
	rm -rf slurm*
	rm -rf ~*
	rm -rf *~
	rm -rf \#*
	rm -rf *\#

run:
	pipenv run python scheduler.py $(ARGS)

test: 
	pipenv run python -m unittest

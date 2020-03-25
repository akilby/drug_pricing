ARGS=main.py

clean:
	rm ~*
	rm slurm*
	rm \#*
	rm *\#

run:
	python $(ARGS)

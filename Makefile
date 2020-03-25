ARGS=main.py

clean:
	rm \#*
	rm *\#
	rm slurm*
	rm ~*

run:
	python $(ARGS)

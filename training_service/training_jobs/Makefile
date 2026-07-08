.PHONY: test
.DEFAULT_GOAL := test

test:
	@PYTHONPATH=$(CURDIR) python3 -m unittest discover -s ../test/training_jobs/tests -p '*_test.py'

.PHONY: help init test

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  init      to install requirements.txt"
	@echo "  test      to run unit test"

init:
    pip install -r requirements.txt

test:
    py.test tests

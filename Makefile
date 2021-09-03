.PHONY: clean venv

all: clean venv

clean:
	rm -rf venv

venv:
	py -m venv venv
	venv\Scripts\python.exe -m pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

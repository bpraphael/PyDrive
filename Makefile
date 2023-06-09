.PHONY: clean posix windows

all:
	@echo Please make either \'posix\' or \'windows\' target.

posix:
	@$(MAKE) venv PY='python3' VPY='venv/bin/python'

windows:
	@$(MAKE) venv PY='python3' VPY='venv\Scripts\python.exe'

clean:
	rm -rf venv

venv:
	$(PY) -m venv venv
	$(VPY) -m pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

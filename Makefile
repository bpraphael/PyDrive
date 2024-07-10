.PHONY: clean posix windows

all:
	@echo Please make either \'posix\' or \'windows\' target.

posix: PY := python3
posix: VPY := venv/bin/python
posix: venv

windows: PY := python3
windows: VPY := venv\Scripts\python.exe
windows: venv

clean:
	rm -rf venv

venv:
	$(PY) -m venv venv
	$(VPY) -m pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

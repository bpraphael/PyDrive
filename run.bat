@echo off

venv\Scripts\python.exe upload.py --ask-source --ask-dest --dest "fotos-videos" --no-max-size
rem venv\Scripts\python.exe checkdups.py --ask-dest --dest "fotos-videos"
pause

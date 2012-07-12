#!/bin/sh
./prochsi.py content &
chromium-browser --proxy-server="localhost:8080" --user-data-dir="chromium-proxy-user" &

#!/bin/bash

# Define the script path
SCRAPER_SCRIPT="vibhu/sensibull/scraper.py"
LOG_FILE="vibhu/sensibull/scraper.log"
CWD="/Users/vibhu/zd/pykiteconnect-master"

# Check if the process is running
if pgrep -f "$SCRAPER_SCRIPT" > /dev/null
then
    echo "$(date): Scraper is running."
else
    echo "$(date): Scraper stopped. Restarting..."
    cd "$CWD"
    nohup python3 "$SCRAPER_SCRIPT" >> "$LOG_FILE" 2>&1 &
fi

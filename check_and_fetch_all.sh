#!/bin/bash
# Script to check for missing user activities and fetch them, then process charts

cd "/Users/shubham.1/Applications/untitled folder"

echo "Step 1: Checking for missing user activities..."
node fetch_missing_user_activities.js

echo ""
echo "Step 2: Processing charts for any newly fetched activities..."
python3 fix_incomplete_markets.py

echo ""
echo "Done!"

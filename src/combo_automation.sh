#!/bin/bash

# Step 1: Run the Python script to generate the JSON files
python3 src/combo_app.py

# Step 2: Run the JavaScript script to analyze the generated JSON files
node src/main_daily.js

# Step 3: Assuming your JS script outputs a new JSON file,
# merge that output into the combined_output.json
# (Assuming you already have code logic to add it into categories)

# Step 4: Stage the changes for GitHub
cd src
git add combined_output.json

# Step 5: Commit the changes
git commit -m "Daily update of combined_output.json"

# Step 6: Push to GitHub
git push origin main

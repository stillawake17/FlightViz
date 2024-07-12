#!/bin/bash

# Add the updated file to git
git add monthly_data.json

# Commit the changes
git commit -m "Update monthly data with new daily summary"

# Push the changes to the repository
git push origin main  # Adjust branch name as necessary

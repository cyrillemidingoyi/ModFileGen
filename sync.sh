#!/bin/bash

# Define remotes
GITHUB_REMOTE="origin"
GITLAB_REMOTE="gitlab"

# Fetch latest changes from GitHub
echo "Fetching changes from GitHub..."
git fetch $GITHUB_REMOTE

# Push changes to GitLab
echo "Pushing changes to GitLab..."
git push $GITLAB_REMOTE --all
git push $GITLAB_REMOTE --tags

echo "Synchronization complete!"
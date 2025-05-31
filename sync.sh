#!/bin/bash


GITLAB_REMOTE="gitlab"
EXCLUDED_TAGS=("v1.0.0")  # <-- liste des tags à ignorer

echo "🔁 Pushing 'main' branch to GitLab (force)..."
git push $GITLAB_REMOTE main --force

echo "🏷️  Pushing tags (except excluded ones)..."
for tag in $(git tag); do
  skip_tag=false
  for exclude in "${EXCLUDED_TAGS[@]}"; do
    if [[ "$tag" == "$exclude" ]]; then
      echo "⛔ Skipping excluded tag: $tag"
      skip_tag=true
      break
    fi
  done
  if [ "$skip_tag" = false ]; then
    echo "✅ Pushing tag: $tag"
    git push $GITLAB_REMOTE $tag
  fi
done

echo "✅ Synchronization complete!"

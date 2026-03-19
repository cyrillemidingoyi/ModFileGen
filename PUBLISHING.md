# Publishing to PyPI

This document explains how to publish ModFileGen to PyPI (Python Package Index).

## Prerequisites

1. **GitHub Repository Setup:**
   - Repository: https://github.com/CropModelingPlatform/ModFileGen
   - Ensure you have admin access

2. **PyPI Account:**
   - Create account on https://pypi.org
   - Create account on https://test.pypi.org (for testing)

3. **Configure Trusted Publishing (Recommended):**
   
   This allows GitHub Actions to publish without API tokens.
   
   ### On PyPI:
   1. Go to https://pypi.org/manage/account/publishing/
   2. Add a new pending publisher:
      - **PyPI Project Name:** `modfilegen`
      - **Owner:** `CropModelingPlatform`
      - **Repository name:** `ModFileGen`
      - **Workflow name:** `publish-to-pypi.yml`
      - **Environment name:** `pypi`
   
   ### On TestPyPI (for testing):
   1. Go to https://test.pypi.org/manage/account/publishing/
   2. Add same configuration with environment name: `testpypi`

## Publishing Process

### Method 1: Automatic (via GitHub Release) - RECOMMENDED

1. **Update version in `src/modfilegen/version.py`:**
   ```python
   __version__ = "1.0.0"  # New version
   ```

2. **Commit and push:**
   ```bash
   git add src/modfilegen/version.py
   git commit -m "Bump version to 1.0.0"
   git push origin main
   ```

3. **Create a GitHub Release:**
   ```bash
   # Tag the commit
   git tag v1.0.0
   git push origin v1.0.0
   ```
   
   Or via GitHub UI:
   - Go to https://github.com/CropModelingPlatform/ModFileGen/releases/new
   - Tag: `v1.0.0`
   - Title: `Release 1.0.0`
   - Description: List of changes
   - Click "Publish release"

4. **Workflow runs automatically:**
   - GitHub Actions workflow triggers
   - Package is built and tested
   - Published to PyPI with trusted publishing

### Method 2: Manual Testing (TestPyPI)

1. **Trigger workflow manually:**
   - Go to Actions → "Publish to PyPI"
   - Click "Run workflow"
   - Select "Publish to TestPyPI"
   
2. **Test installation:**
   ```bash
   pip install --index-url https://test.pypi.org/simple/ modfilegen
   ```

### Method 3: Local Build and Publish (Manual)

```bash
# Install build tools
pip install build twine

# Build distributions
python -m build

# Check the build
twine check dist/*

# Upload to TestPyPI (testing)
twine upload --repository testpypi dist/*

# Upload to PyPI (production)
twine upload dist/*
```

## Post-Publication

### Verify on PyPI
- Check: https://pypi.org/project/modfilegen/
- Test installation: `pip install modfilegen`

### Update Documentation
- Ensure ReadTheDocs builds successfully
- Update README with installation instructions

### Announce Release
- Create GitHub Release notes
- Update HISTORY.rst with changelog

## Versioning

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes

## Checklist Before Publishing

- [ ] All tests pass in CI
- [ ] Version number updated in `version.py`
- [ ] HISTORY.rst updated with changes
- [ ] README.rst accurate and complete
- [ ] Dependencies specified correctly
- [ ] License file present
- [ ] CITATION.cff updated

## Troubleshooting

### "Project name already exists"
- The name `modfilegen` is already registered
- Contact current owner or choose different name

### "Trusted publishing not configured"
- Follow Prerequisites section to set up trusted publishing
- Or use API token method (less secure)

### "Build failed"
- Check `setup.py` has correct metadata
- Ensure all required files exist (README.rst, HISTORY.rst)
- Run `python -m build` locally to test

### "Upload forbidden"
- Verify you have maintainer rights on PyPI
- Check trusted publishing configuration matches exactly

## Continuous Integration

The `.github/workflows/publish-to-pypi.yml` workflow:
- **Triggers:** On GitHub Release publication
- **Steps:**
  1. Build wheel and source distribution
  2. Check package with twine
  3. Publish to PyPI with trusted publishing
- **Manual trigger:** Via Actions tab for TestPyPI

## Resources

- PyPI: https://pypi.org/project/modfilegen/
- TestPyPI: https://test.pypi.org/project/modfilegen/
- Packaging Guide: https://packaging.python.org/
- Trusted Publishing: https://docs.pypi.org/trusted-publishers/

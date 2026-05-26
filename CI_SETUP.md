# 🔄 Continuous Integration Setup for APSIM Tests

This guide explains how to set up and use GitHub Actions to automatically test the APSIM converter.

## 📋 Overview

The CI pipeline automatically runs tests on:
- Every push to `main` or `develop` branches
- Every pull request targeting `main` or `develop`
- Manual trigger via GitHub Actions UI
- Changes to APSIM converter code or tests

## 🚀 Quick Start

### 1. Enable GitHub Actions

The workflow file is already created at `.github/workflows/apsim-tests.yml`. GitHub Actions will automatically detect and run it when you:

```bash
# Commit and push the workflow file
git add .github/workflows/apsim-tests.yml
git commit -m "Add CI workflow for APSIM tests"
git push
```

### 2. Verify Test Databases

Make sure your test databases are committed to the repository:

```bash
# Check if databases exist
ls -lh tests/data/MasterInput_bon_test.db
ls -lh tests/data/ModelsDictionaryArise.db

# Add them if not already tracked
git add tests/data/*.db
git commit -m "Add test databases for CI"
git push
```

### 3. Monitor Test Runs

1. Go to your GitHub repository
2. Click on "Actions" tab
3. You'll see test runs with status indicators:
   - ✅ Green check: All tests passed
   - ❌ Red X: Tests failed
   - 🟡 Yellow circle: Tests running

## 📊 Workflow Details

### Test Matrix

Tests run on multiple Python versions in parallel:
- Python 3.9
- Python 3.10
- Python 3.11
- Python 3.12

### Test Jobs

#### 1. **test-apsim** (Main Tests)
Runs on every push/PR:
- Weather creation tests (`test_weather_creation.py`)
- Main converter tests (`test_apsim_main.py`)
- Unit tests for weather, soil, and management converters

**Duration:** ~3-8 minutes depending on Python version

#### 2. **test-examples** (Examples)
Runs only on push to main branches:
- Weather usage examples (`example_weather_usage.py`)
- Verification of example outputs

**Duration:** ~2-5 minutes

#### 3. **code-quality** (Quality Checks)
Runs on every push/PR:
- Flake8 linting
- Python syntax validation

**Duration:** ~1 minute

### Artifacts

Test outputs are saved as artifacts for 7 days:
- `apsim-test-results-py3.9` (sample outputs from Python 3.9)
- `apsim-test-results-py3.10` (sample outputs from Python 3.10)
- `apsim-test-results-py3.11` (sample outputs from Python 3.11)
- `apsim-test-results-py3.12` (sample outputs from Python 3.12)

To download artifacts:
1. Go to Actions tab
2. Click on a completed workflow run
3. Scroll to "Artifacts" section
4. Download the desired artifact

## 🔧 Customization

### Change Tested Python Versions

Edit `.github/workflows/apsim-tests.yml`:

```yaml
strategy:
  matrix:
    python-version: ['3.9', '3.12']  # Test only 3.9 and 3.12
```

### Skip CI for Specific Commits

Add `[skip ci]` to your commit message:

```bash
git commit -m "Update documentation [skip ci]"
```

### Run Tests Manually

1. Go to Actions tab
2. Click "APSIM Converter Tests" workflow
3. Click "Run workflow" button
4. Select branch and click "Run workflow"

### Add More Tests

Edit the workflow to add new test files:

```yaml
- name: 🧪 Run new test suite
  run: |
    python tests/apsim/test_new_feature.py
```

### Change Test Timeout

If tests take longer, increase the timeout:

```yaml
- name: 🧪 Run main APSIM converter tests
  run: python tests/apsim/test_apsim_main.py
  timeout-minutes: 20  # Increased from 10
```

## 🐛 Troubleshooting

### Tests Fail in CI but Pass Locally

**Database Size Issues:**
- GitHub Actions runners have limited disk space
- Workflow keeps only 10 sample simulation directories

**Solution:** Reduce test data size or split into multiple jobs

### Database Not Found Error

```
FileNotFoundError: tests/data/MasterInput_bon_test.db
```

**Solutions:**
1. Verify database is committed: `git ls-files tests/data/*.db`
2. Check .gitignore doesn't exclude .db files
3. Manually add: `git add -f tests/data/*.db`

### Timeout Errors

```
Error: The operation was canceled.
```

**Solutions:**
1. Check test hangs due to missing database indexes
2. Increase timeout in workflow: `timeout-minutes: 20`
3. Add database indexes in workflow:

```yaml
- name: 📊 Create database indexes
  run: |
    sqlite3 tests/data/MasterInput_bon_test.db << EOF
    CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);
EOF
```

### Memory Issues

**Error:** `MemoryError` or `Killed`

**Solutions:**
1. Use `jobs.<job_id>.runs-on: ubuntu-latest` with more memory
2. Reduce batch sizes in tests
3. Split into multiple smaller jobs

### Import Errors

```
ModuleNotFoundError: No module named 'modfilegen'
```

**Solution:** Install package in editable mode:

```yaml
- name: 📦 Install package
  run: pip install -e .
```

## 📈 Test Results Dashboard

After each run, check the test summary:

1. Click on a workflow run
2. Scroll to bottom to see "Summary"
3. View:
   - Number of files generated
   - Weather files created
   - Environment details
   - Test status for each Python version

## 🔒 Security Considerations

### Database Files
- Test databases should NOT contain sensitive data
- Use sanitized/anonymized data for CI
- Keep database files small (<100MB recommended)

### Secrets
If you need API keys or credentials:

```yaml
steps:
  - name: Set up credentials
    env:
      API_KEY: ${{ secrets.API_KEY }}
    run: echo "API_KEY=$API_KEY" >> $GITHUB_ENV
```

Add secrets at: `Repository Settings → Secrets and variables → Actions`

## 📝 Best Practices

### ✅ Do:
- Keep test databases small and representative
- Use database indexes for performance
- Set reasonable timeouts (5-10 minutes)
- Upload only sample artifacts to save space
- Run quick tests first (fail fast)
- Use matrix for Python version testing

### ❌ Don't:
- Commit large output files
- Run full integration tests on every commit
- Store credentials in workflow files
- Generate thousands of files in CI
- Use absolute paths in tests

## 🔗 Related Documentation

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Python Setup Action](https://github.com/actions/setup-python)
- [APSIM Test Documentation](tests/apsim/README.md)
- [APSIM Migration Guide](APSIM_MIGRATION_GUIDE.md)

## 🆘 Getting Help

If tests fail in CI:

1. **Check logs:** Click on failed step to see detailed output
2. **Download artifacts:** Check generated files for issues
3. **Compare with local:** Run same command locally
4. **Check recent changes:** Review commits since last successful run

**Common log locations:**
- Test output: In each test step
- File generation: "Check generated files" step
- Database info: "Verify test databases exist" step

## 🎯 Success Criteria

Your CI is working correctly when:
- ✅ All tests pass on multiple Python versions
- ✅ Weather files are generated successfully
- ✅ Main converter generates expected file counts
- ✅ Examples run without errors
- ✅ Artifacts are uploaded successfully
- ✅ Test summary shows expected results

## 📅 Maintenance

**Monthly tasks:**
- Review and clean up old artifacts
- Update Python versions if needed
- Check for deprecated GitHub Actions
- Update dependencies if security issues found

**Before releases:**
- Ensure all tests pass on main branch
- Review test coverage
- Update test data if schema changed
- Verify examples still work

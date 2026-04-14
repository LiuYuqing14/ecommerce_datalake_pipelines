# Docker Image Versioning Strategy

## Overview

Docker images are automatically versioned using Git metadata (commit SHA, branch, tags). This provides full traceability from deployed containers back to source code.

## Version Format

**Version string** is determined by:
- **Git tag** (if on tagged commit): `v1.0.0`
- **Branch + commit** (otherwise): `main-a1b2c3d`

**Example versions**:
```
v1.0.0           # Released version (Git tag)
v1.0.1           # Patch release (Git tag)
main-a1b2c3d     # Development build from main branch
feature-xyz-e4f5 # Feature branch build
```

## Image Metadata

Each image contains OCI labels with Git metadata:

```dockerfile
org.opencontainers.image.version=main-a1b2c3d
org.opencontainers.image.revision=a1b2c3d
com.ecom.git.branch=main
com.ecom.git.commit=a1b2c3d
```

**Inspect image metadata**:
```bash
docker inspect ecom-datalake-pipeline:latest | jq '.[0].Config.Labels'
```

## Runtime Version Access

Version information is available inside containers via environment variables:

```bash
# Inside container:
echo $PIPELINE_VERSION  # main-a1b2c3d
echo $GIT_COMMIT        # a1b2c3d
echo $GIT_BRANCH        # main

# Or use the version script:
ecomlake version
```

**Output**:
```
============================================================
E-commerce Data Pipeline - Version Information
============================================================
Version:    main-a1b2c3d
Git Commit: a1b2c3d
Git Branch: main
Source:     docker
============================================================
```

## Local Development

### Build Versioned Image

```bash
# Build with Git version tags
ecomlake deploy build-versioned

# Output:
# Git Commit: a1b2c3d
# Git Branch: main
# Version: main-a1b2c3d
# Built images:
#   - ecom-datalake-pipeline:main-a1b2c3d
#   - ecom-datalake-pipeline:latest
```

### Check Version

```bash
# From Git repository (local dev):
ecomlake version

# From Docker container:
docker-compose exec airflow-scheduler ecomlake version
```

## CI/CD - GitHub Actions

### Automatic Builds

The [docker-build.yml](.github/workflows/docker-build.yml) workflow automatically builds and pushes images to GitHub Container Registry on:

- **Push to main** → `ghcr.io/g-schumacher44/ecom_datalake_pipelines:main`
- **Pull requests** → Build only (no push)
- **Git tags** → `ghcr.io/g-schumacher44/ecom_datalake_pipelines:v1.0.0`

### Image Tags

GitHub Actions creates multiple tags per build:

| Trigger | Tags Created |
|---------|--------------|
| Push to `main` | `main`, `main-a1b2c3d`, `latest` |
| Tag `v1.0.0` | `v1.0.0`, `1.0`, `1`, `latest` |
| PR #123 | `pr-123` (build only, no push) |

## GCP Artifact Registry

### Push to Artifact Registry

```bash
# Push versioned image to GCP
ecomlake deploy push-image-versioned PROJECT_ID=my-gcp-project

# Creates:
# us-central1-docker.pkg.dev/my-gcp-project/airflow-images/ecom-datalake-pipeline:main-a1b2c3d
# us-central1-docker.pkg.dev/my-gcp-project/airflow-images/ecom-datalake-pipeline:latest
```

### Cloud Composer Deployment

**Option 1: Use versioned tag (recommended)**
```bash
# Deploy specific version to Cloud Composer
gcloud composer environments update my-env \
  --location us-central1 \
  --airflow-image us-central1-docker.pkg.dev/my-gcp-project/airflow-images/ecom-datalake-pipeline:v1.0.0
```

**Option 2: Use latest (dev/staging)**
```bash
gcloud composer environments update my-env \
  --location us-central1 \
  --airflow-image us-central1-docker.pkg.dev/my-gcp-project/airflow-images/ecom-datalake-pipeline:latest
```

## Release Workflow

### 1. Development

```bash
# Work on feature branch
git checkout -b feature/new-transform
# ... make changes ...
git commit -m "Add new transform"
git push origin feature/new-transform

# CI builds: ecom-datalake-pipeline:feature-new-transform-a1b2c3d
```

### 2. Merge to Main

```bash
# After PR approval, merge to main
git checkout main
git pull

# CI builds: ecom-datalake-pipeline:main-e4f5g6h
# CI tags:   ecom-datalake-pipeline:latest
```

### 3. Create Release

```bash
# Tag release
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0

# CI builds: ecom-datalake-pipeline:v1.0.0
# CI tags:   ecom-datalake-pipeline:1.0, ecom-datalake-pipeline:1, ecom-datalake-pipeline:latest
```

### 4. Deploy to Production

```bash
# Deploy tagged version to prod Cloud Composer
ecomlake deploy push-image-versioned PROJECT_ID=my-prod-project

gcloud composer environments update prod-env \
  --location us-central1 \
  --airflow-image us-central1-docker.pkg.dev/my-prod-project/airflow-images/ecom-datalake-pipeline:v1.0.0
```

## Troubleshooting

### Check Image Version

```bash
# List all local images with tags
docker images ecom-datalake-pipeline

# Inspect labels
docker inspect ecom-datalake-pipeline:latest | jq '.[0].Config.Labels'
```

### Version Mismatch

If version shows `unknown`:
1. Ensure you're building from a Git repository
2. Check that build args are passed: `ecomlake deploy build-versioned`
3. Verify Git is installed in build environment

### Docker Compose Not Using Versioned Image

Update [docker-compose.yml](../../docker-compose.yml) to use versioned image:

```yaml
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
    args:
      GIT_COMMIT: ${GIT_COMMIT:-unknown}
      GIT_BRANCH: ${GIT_BRANCH:-unknown}
      VERSION: ${VERSION:-latest}
  image: ecom-datalake-pipeline:${VERSION:-latest}
```

Then build:
```bash
export VERSION=$(git describe --tags --always)
docker-compose build
```

## Best Practices

1. **Always tag releases** - Use semantic versioning (`v1.0.0`, `v1.0.1`)
2. **Pin production images** - Deploy specific versions, not `:latest`
3. **Use `:latest` for dev/staging** - Automatic updates for non-prod
4. **Check version before deploy** - `ecomlake version` or inspect image labels
5. **Keep Git tags clean** - Only tag stable releases, not every commit

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>

# CI/CD Setup for Kafka Connect .NET

This document describes the CI/CD pipeline setup for building, testing, and publishing the Kafka Connect .NET project to GitHub Packages and GitHub Container Registry (GHCR).

## Overview

The CI/CD pipeline consists of three main workflows:

1. **Build and Publish** (`build-and-publish.yml`) - Main workflow for continuous integration
2. **Publish NuGet Packages** (`publish-nuget.yml`) - Dedicated workflow for NuGet package publishing
3. **Docker Publish** (`docker-publish.yml`) - Dedicated workflow for Docker image publishing

## Files Created

### Docker Configuration
- `Dockerfile` - Multi-stage Docker build for the application
- `nuget.config` - NuGet configuration using nuget.org and GitHub Packages

### GitHub Actions Workflows
- `.github/workflows/build-and-publish.yml` - Main CI/CD pipeline
- `.github/workflows/publish-nuget.yml` - NuGet package publishing
- `.github/workflows/docker-publish.yml` - Docker image publishing

## Workflow Details

### 1. Build and Publish Workflow

**Triggers:**
- Push to `main` or `develop` branches
- Push of version tags (`v*`)
- Pull requests to `main`

**Jobs:**
- `build-and-test`: Builds and tests the solution
- `publish-nuget`: Publishes NuGet packages to GitHub Packages (only on main/tags)
- `build-and-push-docker`: Builds and pushes Docker image to GHCR (only on main/tags)

### 2. Publish NuGet Packages Workflow

**Triggers:**
- Release published
- Manual workflow dispatch

**Features:**
- Supports manual version specification
- Handles prerelease versions
- Publishes all plugin packages
- Uploads packages as artifacts

### 3. Docker Publish Workflow

**Triggers:**
- Push to `main` branch
- Push of version tags (`v*`)
- Release published
- Manual workflow dispatch

**Features:**
- Multi-platform builds
- Docker layer caching
- Artifact attestation
- Flexible tagging strategy

## Package Publishing

### NuGet Packages Published:
- `Kafka.Connect.Plugin` - Base plugin framework
- All plugin packages in `src/Plugins/` directory

### Docker Images Published:
- Main application image to `ghcr.io/{owner}/{repository}`

## Configuration Requirements

### GitHub Repository Settings

1. **Packages Permission**: Ensure the repository has package write permissions
2. **Actions Permission**: Enable GitHub Actions for the repository

### Secrets (Auto-configured)
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions

### Environment Variables
The workflows use these environment variables:
- `GITHUB_TOKEN` - For authentication
- `GITHUB_USERNAME` - GitHub actor (automatically set)
- `BUILD_VERSION` - Version for builds (automatically determined)

## Version Strategy

### Automatic Versioning:
- **Tags**: Uses the tag version (removes 'v' prefix)
- **Main branch**: Uses `1.0.0-preview.{run_number}`
- **Manual**: Uses specified version

### Tag Format:
- Use semantic versioning: `v1.0.0`, `v1.2.3-beta`, etc.
- The 'v' prefix is automatically removed for package versions

## Usage

### Publishing a Release:

1. **Create a tag:**
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. **Create a GitHub release:**
   - Go to GitHub repository → Releases → Create new release
   - Select the tag created above
   - Publish the release

### Manual Publishing:

1. **NuGet Packages:**
   - Go to Actions → Publish NuGet Packages → Run workflow
   - Specify version and prerelease flag

2. **Docker Image:**
   - Go to Actions → Build and Publish Docker Image → Run workflow
   - Specify custom tag if needed

## Package Consumption

### Using NuGet Packages:

1. **Add GitHub Packages source:**
   ```xml
   <packageSources>
     <add key="github" value="https://nuget.pkg.github.com/{owner}/index.json" />
   </packageSources>
   ```

2. **Add authentication:**
   ```xml
   <packageSourceCredentials>
     <github>
       <add key="Username" value="{github_username}" />
       <add key="ClearTextPassword" value="{github_token}" />
     </github>
   </packageSourceCredentials>
   ```

### Using Docker Image:

```bash
docker pull ghcr.io/{owner}/{repository}:latest
docker run ghcr.io/{owner}/{repository}:latest
```

## Troubleshooting

### Common Issues:

1. **Package Push Fails**: Ensure repository has package write permissions
2. **Docker Build Fails**: Check Dockerfile paths and build context
3. **Authentication Issues**: Verify GITHUB_TOKEN permissions

### Debugging:

1. Check workflow logs in GitHub Actions tab
2. Verify package sources in nuget.config
3. Ensure all required files are present in repository

## Migration from CBA Artifactory

### Changes Made:
- Replaced CBA Docker images with Microsoft official images
- Changed from Artifactory to GitHub Packages for NuGet publishing
- Updated authentication from API keys to GitHub tokens
- Simplified NuGet configuration to use nuget.org

### Benefits:
- No dependency on internal CBA infrastructure
- Simplified authentication using GitHub tokens
- Public availability of packages and images
- Integrated with GitHub repository
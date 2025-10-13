FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS base

ARG BUILD_VERSION=1.0.0.1
ARG GITHUB_TOKEN
ARG GITHUB_USERNAME
ARG GITHUB_PACKAGES_SOURCE=https://nuget.pkg.github.com/$GITHUB_USERNAME/index.json

ENV GITHUB_TOKEN=$GITHUB_TOKEN
ENV GITHUB_USERNAME=$GITHUB_USERNAME
ENV GITHUB_PACKAGES_SOURCE=$GITHUB_PACKAGES_SOURCE

WORKDIR /src

# Copy only src directory and config files (no tests)
COPY ./src ./src
COPY ./nuget.config ./nuget.config

# Release build stage
FROM base AS release
RUN echo "=== RELEASE MODE: Building with NuGet packages ===" && \
    # Build and pack Kafka.Connect.Plugin first
    cd /src/src/Kafka.Connect.Plugin && \
    dotnet restore Kafka.Connect.Plugin.csproj --configfile /src/nuget.config && \
    dotnet build Kafka.Connect.Plugin.csproj /p:Version=$BUILD_VERSION --configuration Release --no-restore && \
    dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs && \
    dotnet nuget push ./nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE && \
    \
    # Build all plugins using Kafka.Connect.Plugins.sln
    cd /src/src/Plugins && \
    echo "Adding Kafka.Connect.Plugin version $BUILD_VERSION to Directory.Packages.props" && \
    sed -i "/<\/ItemGroup>/i\\    <PackageVersion Include=\"Kafka.Connect.Plugin\" Version=\"$BUILD_VERSION\" />" /src/src/Directory.Packages.props && \
    echo "Waiting for package to be available in registry..." && \
    sleep 10 && \
    echo "Restoring all plugins using Kafka.Connect.Plugins.sln in Release configuration" && \
    dotnet restore Kafka.Connect.Plugins.sln /p:Configuration=Release --configfile /src/nuget.config --no-cache --force && \
    dotnet build Kafka.Connect.Plugins.sln /p:Version=$BUILD_VERSION --configuration Release --no-restore && \
    \
    # Pack all plugins
    mkdir -p nupkgs && \
    dotnet pack Kafka.Connect.Plugins.sln /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs && \
    \
    # Publish all plugin packages
    for package in ./nupkgs/*.${BUILD_VERSION}.nupkg; do \
        if [ -f "$package" ]; then \
            echo "Publishing: $package" && \
            dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE; \
        fi; \
    done && \
    \
    # Build main application
    cd /src/src/Kafka.Connect && \
    dotnet restore Kafka.Connect.csproj /p:Configuration=Release --configfile /src/nuget.config && \
    dotnet publish Kafka.Connect.csproj /p:Version=$BUILD_VERSION -c Release -o /app/out --no-restore

# Runtime stage for release builds
FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app

# Copy the published application from release stage
COPY --from=release /app/out ./

# Copy release plugin outputs
COPY --from=release /src/src/Plugins/ /tmp/plugins/

RUN mkdir -p ./plugins && \
    echo "Setting up Release plugins..." && \
    for plugin_dir in /tmp/plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            plugin_bin_dir="$plugin_dir/bin/Release/net8.0"; \
            if [ -d "$plugin_bin_dir" ]; then \
                echo "Copying Release build for plugin: $plugin_name"; \
                mkdir -p "./plugins/$plugin_name"; \
                cp -r "$plugin_bin_dir"/* "./plugins/$plugin_name/"; \
            else \
                echo "Warning: No Release build found for plugin: $plugin_name"; \
            fi; \
        fi; \
    done && \
    rm -rf /tmp/plugins

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

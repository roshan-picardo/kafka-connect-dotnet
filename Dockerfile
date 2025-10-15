FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS base

ARG BUILD_VERSION=1.0.0.1
ARG GITHUB_TOKEN
ARG GITHUB_USERNAME
ARG GITHUB_PACKAGES_SOURCE=https://nuget.pkg.github.com/$GITHUB_USERNAME/index.json

ENV GITHUB_TOKEN=$GITHUB_TOKEN
ENV GITHUB_USERNAME=$GITHUB_USERNAME
ENV GITHUB_PACKAGES_SOURCE=$GITHUB_PACKAGES_SOURCE

WORKDIR /src

# Copy contents of src directory and config files (no tests)
COPY ./src/* ./
COPY ./nuget.config ./nuget.config

# Release build stage
FROM base AS release
RUN cd /src && \
    mkdir -p nupkgs && \
    cd Kafka.Connect.Plugin && \
    dotnet restore Kafka.Connect.Plugin.csproj /p:Configuration=Release --configfile /src/nuget.config && \
    dotnet build Kafka.Connect.Plugin.csproj /p:Version=$BUILD_VERSION --configuration Release --no-restore && \
    dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output /src/nupkgs && \
    dotnet nuget push /src/nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE && \
    cd /src && \
    sed -i "/<\/ItemGroup>/i\\    <PackageVersion Include=\"Kafka.Connect.Plugin\" Version=\"$BUILD_VERSION\" />" Directory.Packages.props && \
    sleep 10 && \
    dotnet restore Kafka.Connect/Kafka.Connect.csproj /p:Configuration=Release --configfile /src/nuget.config --no-cache --force && \
    dotnet publish Kafka.Connect/Kafka.Connect.csproj /p:Version=$BUILD_VERSION -c Release -o /app --no-restore && \
    mkdir -p /app/plugins && \
    for plugin_dir in Plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            project_name=$(basename "$plugin_dir"); \
            project_file="$plugin_dir/$project_name.csproj"; \
            if [ -f "$project_file" ]; then \
                dotnet restore "$project_file" /p:Configuration=Release --configfile /src/nuget.config --no-cache --force; \
                dotnet build "$project_file" /p:Version=$BUILD_VERSION --configuration Release --no-restore; \
                dotnet pack "$project_file" /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output /src/nupkgs ; \
                dotnet publish "$project_file" -c Release -o "/app/plugins/$plugin_name" --no-restore; \
            fi; \
        fi; \
    done; \
    for package in /src/nupkgs/*.$BUILD_VERSION.nupkg; do \
        if [ -f "$package" ]; then \
            dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE; \
        fi; \
    done;

# Runtime stage for release builds
FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app

# Copy the published application and plugins from release stage
COPY --from=release /app ./

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS build

ARG BUILD_VERSION=1.0.0.1
ARG GITHUB_TOKEN
ARG GITHUB_USERNAME
ARG GITHUB_PACKAGES_SOURCE=https://nuget.pkg.github.com/roshan-picardo/index.json
ARG PUBLISH=true

ENV GITHUB_TOKEN=$GITHUB_TOKEN
ENV GITHUB_USERNAME=$GITHUB_USERNAME
ENV GITHUB_PACKAGES_SOURCE=$GITHUB_PACKAGES_SOURCE

WORKDIR /src
COPY ./src/Kafka.Connect.Plugin ./Kafka.Connect.Plugin
COPY ./src/Kafka.Connect ./Kafka.Connect
COPY ./src/Plugins ./Plugins
COPY ./src/Directory.Packages.props ./Directory.Packages.props
COPY ./src/Kafka.sln ./Kafka.sln
COPY ./nuget.config ./nuget.config

# Build and pack Kafka.Connect.Plugin first
WORKDIR /src/Kafka.Connect.Plugin
RUN dotnet restore --configfile /src/nuget.config
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        dotnet nuget push ./nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
    fi

# Clear NuGet cache and update Directory.Packages.props with the new version
WORKDIR /src/Plugins
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        echo "Clearing NuGet cache to ensure fresh package resolution..."; \
        dotnet nuget locals all --clear; \
        echo "Adding Kafka.Connect.Plugin version $BUILD_VERSION to Directory.Packages.props"; \
        sed -i "/<\/ItemGroup>/i\\    <PackageVersion Include=\"Kafka.Connect.Plugin\" Version=\"$BUILD_VERSION\" />" /src/Directory.Packages.props; \
        sleep 5; \
    fi
RUN for plugin_dir in */; do \
        if [ -d "$plugin_dir" ] && [ -f "$plugin_dir"*.csproj ]; then \
            echo "Processing plugin: $plugin_dir"; \
            cd "$plugin_dir"; \
            if [[ "$PUBLISH" == "true" ]] ; then \
                echo "Restoring with specific Kafka.Connect.Plugin version: $BUILD_VERSION"; \
                dotnet restore --configfile /src/nuget.config --force --verbosity detailed; \
            else \
                dotnet restore --configfile /src/nuget.config; \
            fi; \
            dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore; \
            if [[ "$PUBLISH" == "true" ]] ; then \
                dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ../nupkgs; \
            fi; \
            cd ..; \
        fi; \
    done

# Publish all plugin packages
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        mkdir -p nupkgs; \
        for package in ./nupkgs/*.${BUILD_VERSION}.nupkg; do \
            if [ -f "$package" ]; then \
                echo "Publishing: $package"; \
                dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
            fi; \
        done \
    fi

# Build the main application
WORKDIR /src/Kafka.Connect
RUN dotnet restore --configfile /src/nuget.config
RUN dotnet publish /p:Version=$BUILD_VERSION -c Release -o out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app
COPY --from=build /src/Kafka.Connect/out .

# Copy all plugin outputs dynamically
COPY --from=build /src/Plugins/ /tmp/plugins/
RUN mkdir -p ./plugins && \
    for plugin_dir in /tmp/plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            if [ -d "$plugin_dir/bin/Release/net8.0" ]; then \
                cp -r "$plugin_dir/bin/Release/net8.0" "./plugins/$plugin_name/"; \
            fi; \
        fi; \
    done && \
    rm -rf /tmp/plugins

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

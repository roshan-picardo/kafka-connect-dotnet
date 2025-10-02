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
COPY ./nuget.debug.config ./nuget.debug.config

# Build and pack Kafka.Connect.Plugin first
WORKDIR /src/Kafka.Connect.Plugin
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet restore --configfile /src/nuget.config ; \
    else \
        dotnet restore --configfile /src/nuget.debug.config ; \
    fi
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore ; \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        dotnet nuget push ./nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
    else \
        dotnet build /p:Version=$BUILD_VERSION --configuration Debug --no-restore ; \
    fi

# Build all plugins using the dedicated solution file
WORKDIR /src/Plugins
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        echo "Adding Kafka.Connect.Plugin version $BUILD_VERSION to Directory.Packages.props"; \
        sed -i "/<\/ItemGroup>/i\\    <PackageVersion Include=\"Kafka.Connect.Plugin\" Version=\"$BUILD_VERSION\" />" /src/Directory.Packages.props; \
        echo "Waiting for package to be available in registry..."; \
        sleep 10; \
        echo "Restoring all plugins using Kafka.Connect.Plugins.sln in Release configuration"; \
        dotnet restore Kafka.Connect.Plugins.sln /p:Configuration=Release --configfile /src/nuget.config --no-cache --force --verbosity detailed ; \
        dotnet build Kafka.Connect.Plugins.sln /p:Version=$BUILD_VERSION --configuration Release --no-restore ; \
    else \
        echo "Restoring all plugins using Kafka.Connect.Plugins.sln in Debug configuration"; \
        dotnet restore Kafka.Connect.Plugins.sln /p:Configuration=Debug --configfile /src/nuget.debug.config --no-cache --force --verbosity detailed ; \
        dotnet build Kafka.Connect.Plugins.sln /p:Version=$BUILD_VERSION --configuration Debug --no-restore ; \
    fi

# Pack all plugins
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        mkdir -p nupkgs; \
        dotnet pack Kafka.Connect.Plugins.sln /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs; \
    fi

# Publish all plugin packages
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        for package in ./nupkgs/*.${BUILD_VERSION}.nupkg; do \
            if [ -f "$package" ]; then \
                echo "Publishing: $package"; \
                dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
            fi; \
        done \
    fi

# Build the main application
WORKDIR /src/Kafka.Connect
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet restore --configfile /src/nuget.config ; \
        dotnet publish /p:Version=$BUILD_VERSION -c Release -o out --no-restore ; \
    else \
        dotnet restore --configfile /src/nuget.debug.config ; \
        dotnet publish /p:Version=$BUILD_VERSION -c Debug -o out --no-restore ; \
    fi

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
            elif [ -d "$plugin_dir/bin/Debug/net8.0" ]; then \
                cp -r "$plugin_dir/bin/Debug/net8.0" "./plugins/$plugin_name/"; \
            fi; \
        fi; \
    done && \
    rm -rf /tmp/plugins

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

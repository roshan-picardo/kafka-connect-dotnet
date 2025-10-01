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

# Restore all projects using the solution file
RUN dotnet restore Kafka.sln /p:Configuration=Release --configfile ./nuget.config

# Build and pack Kafka.Connect.Plugin
WORKDIR /src/Kafka.Connect.Plugin
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        dotnet nuget push ./nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
    fi

# Build and pack all Plugins
WORKDIR /src/Plugins
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        for package in ./nupkgs/*.${BUILD_VERSION}.nupkg; do \
            dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
        done \
    fi

# Build the main application
WORKDIR /src/Kafka.Connect
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

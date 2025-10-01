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
COPY ./src/Kafka.Connect.Plugin ./plugin
COPY ./src/Kafka.Connect ./connect
COPY ./src/Plugins ./plugins
COPY ./src/Directory.Packages.props ./Directory.Packages.props
COPY ./nuget.config ./nuget.config

# Build and pack Kafka.Connect.Plugin
WORKDIR /src/plugin
RUN dotnet restore --configfile /src/nuget.config
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        dotnet nuget push ./nupkgs/Kafka.Connect.Plugin.${BUILD_VERSION}.nupkg --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
    fi

# Build and pack all Plugins
WORKDIR /src/plugins
RUN dotnet restore /p:Configuration=Release --configfile /src/nuget.config
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output ./nupkgs ; \
        for package in ./nupkgs/*.${BUILD_VERSION}.nupkg; do \
            dotnet nuget push "$package" --api-key $GITHUB_TOKEN --source $GITHUB_PACKAGES_SOURCE ; \
        done \
    fi

# Build the main application
WORKDIR /src/connect
RUN dotnet restore /p:Configuration=Release --configfile /src/nuget.config
RUN dotnet publish /p:Version=$BUILD_VERSION -c Release -o out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app
COPY --from=build /src/connect/out .

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"] 
CMD ["--config", "appsettings.json"]
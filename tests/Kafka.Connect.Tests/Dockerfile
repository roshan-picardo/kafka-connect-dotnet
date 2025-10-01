FROM mcr.docker.internal.cba/dotnet/sdk:8.0-alpine AS build

ARG BUILD_VERSION=1.0.0.1
ARG ARTIFACTORY_USER_NAME
ARG ARTIFACTORY_API_KEY
ARG NUGET_SOURCE_EXTERNAL=https://artifactory.internal.cba/api/nuget/nuget.org
ARG PUBLISH=true

ENV ARTIFACTORY_USER_NAME=$ARTIFACTORY_USER_NAME
ENV ARTIFACTORY_API_KEY=$ARTIFACTORY_API_KEY
ENV NUGET_SOURCE_EXTERNAL=$NUGET_SOURCE_EXTERNAL

WORKDIR ./src
COPY ./src/Kafka.Connect.Plugin ./plugin
COPY ./src/Kafka.Connect ./connect
COPY ./src/Plugins ./plugins
COPY ./nuget.build.config /src/nuget.build.config

# Create local NuGet packages directory
RUN mkdir -p /src/local-packages

WORKDIR /src/plugin

RUN dotnet nuget list source
RUN dotnet restore --configfile /src/nuget.build.config --verbosity detailed
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output /src/local-packages ; \
    fi

# plugins
WORKDIR /src/plugins
RUN dotnet restore /p:Configuration=Release --configfile /src/nuget.build.config
RUN dotnet build /p:Version=$BUILD_VERSION --configuration Release --no-restore
RUN if [[ "$PUBLISH" == "true" ]] ; then \
        dotnet pack /p:Version=$BUILD_VERSION --configuration Release --no-build --no-restore --verbosity normal --output /src/local-packages ; \
    fi

WORKDIR /src/connect
RUN dotnet restore /p:Configuration=Release  --configfile /src/nuget.build.config
RUN dotnet publish /p:Version=$BUILD_VERSION -c Release -o out  --no-restore

FROM mcr.docker.internal.cba/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app

# Copy the local packages to the runtime image for reference
COPY --from=build /src/local-packages /app/local-packages
COPY --from=build /src/connect/out .

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]
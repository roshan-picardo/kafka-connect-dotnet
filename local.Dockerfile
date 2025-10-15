FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS base
WORKDIR /src

# Copy only src directory and config files (no tests)
COPY ./src ./src
COPY ./nuget.debug.config ./nuget.config

# Debug build stage
FROM base AS debug
RUN echo "=== DEBUG MODE: Building with proper plugin structure ===" && \
    cd /src/src && \
    echo "1. Restoring Kafka.Connect solution" && \
    dotnet restore Kafka.Connect.sln --configfile /src/nuget.config --no-cache --force && \
    echo "2. Publishing main Kafka.Connect application to /app" && \
    dotnet publish Kafka.Connect/Kafka.Connect.csproj -c Debug -o /app --no-restore && \
    echo "3. Publishing each plugin to /app/plugins/{plugin}" && \
    mkdir -p /app/plugins && \
    for plugin_dir in Plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            echo "Publishing plugin: $plugin_name from $plugin_dir"; \
            dotnet publish "$plugin_dir" -c Debug -o "/app/plugins/$plugin_name" --no-restore; \
        fi; \
    done

# Runtime stage for debug builds
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# Copy the published application and plugins from debug stage
COPY --from=debug /app ./

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS base
WORKDIR /src

# Copy only src directory and config files (no tests)
COPY ./src ./src
COPY ./nuget.debug.config ./nuget.config

# Debug build stage
FROM base AS debug
RUN echo "=== DEBUG MODE: Building source code only (no tests) ===" && \
    cd /src/src && \
    echo "Restoring source solution using Kafka.Connect.sln in Debug configuration" && \
    dotnet restore Kafka.Connect.sln --configfile /src/nuget.config --no-cache --force && \
    echo "Building source solution in Debug configuration" && \
    dotnet build Kafka.Connect.sln --configuration Debug --no-restore && \
    echo "Publishing main application in Debug configuration" && \
    dotnet publish Kafka.Connect/Kafka.Connect.csproj -c Debug -o /app/out --no-restore

# Runtime stage for debug builds
FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app

# Copy the published application from debug stage
COPY --from=debug /app/out ./

# Copy debug plugin outputs
COPY --from=debug /src/src/Plugins/ /tmp/plugins/

RUN mkdir -p ./plugins && \
    echo "Setting up Debug plugins..." && \
    for plugin_dir in /tmp/plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            plugin_bin_dir="$plugin_dir/bin/Debug/net8.0"; \
            if [ -d "$plugin_bin_dir" ]; then \
                echo "Copying Debug build for plugin: $plugin_name"; \
                mkdir -p "./plugins/$plugin_name"; \
                cp -r "$plugin_bin_dir"/* "./plugins/$plugin_name/"; \
            else \
                echo "Warning: No Debug build found for plugin: $plugin_name"; \
            fi; \
        fi; \
    done && \
    rm -rf /tmp/plugins

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]
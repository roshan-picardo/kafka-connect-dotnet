FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS base
WORKDIR /src

# Copy only src directory and config files (no tests)
COPY ./src ./src
COPY ./nuget.debug.config ./nuget.config

# Debug build stage
FROM base AS debug

# Step 1: Restore entire solution (fast parallel restore)
RUN cd /src/src && \
    dotnet restore Kafka.Connect.sln --configfile /src/nuget.config

# Step 2: Build and publish main application
RUN cd /src/src && \
    dotnet publish Kafka.Connect/Kafka.Connect.csproj -c Debug -o /app --no-restore

# Step 3: Build and publish plugins
RUN cd /src/src && \
    mkdir -p /app/plugins && \
    for plugin_dir in Plugins/*/; do \
        if [ -d "$plugin_dir" ]; then \
            plugin_name=$(basename "$plugin_dir" | sed 's/Kafka\.Connect\.//' | tr '[:upper:]' '[:lower:]'); \
            project_name=$(basename "$plugin_dir"); \
            project_file="$plugin_dir/$project_name.csproj"; \
            if [ -f "$project_file" ]; then \
                dotnet publish "$project_file" -c Debug -o "/app/plugins/$plugin_name" --no-restore; \
            fi; \
        fi; \
    done

# Runtime stage for debug builds
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# Copy the published application and plugins from debug stage
COPY --from=debug /app ./

ENTRYPOINT ["dotnet", "Kafka.Connect.dll"]
CMD ["--config", "appsettings.json"]

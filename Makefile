.SILENT:

build:
	-dotnet build ./src/ --configuration Debug --verbosity m

test:
	-dotnet test ./tests/Kafka.Connect.UnitTests --configuration Debug --verbosity m || true

publish:
	-rm -rf ./app/bin
	-dotnet publish ./src/Kafka.Connect/Kafka.Connect.csproj --output ./app/bin --configuration debug --no-build --verbosity m
	-dotnet publish ./src/Plugins/Kafka.Connect.MongoDb/Kafka.Connect.MongoDb.csproj --output ./app/bin/plugins/mongodb --configuration debug --no-build --verbosity m
	-dotnet publish ./src/Plugins/Kafka.Connect.Replicator/Kafka.Connect.Replicator.csproj --output ./app/bin/plugins/replicator --configuration debug --no-build --verbosity m
	-dotnet publish ./src/Plugins/Kafka.Connect.Postgres/Kafka.Connect.Postgres.csproj --output ./app/bin/plugins/postgres --configuration debug --no-build --verbosity m

run:
	-docker compose up -d
	-sleep 5
	-echo ""
	-echo "---------------------"
	-echo "KAFKA CONNECT .NET..."
	-echo "---------------------"
	-echo ""
	-sleep 5
	-dotnet ./app/bin/Kafka.Connect.dll --config ./src/Kafka.Connect/appsettings.json
    
launch: build test publish run

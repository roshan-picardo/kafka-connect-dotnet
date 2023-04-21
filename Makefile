.SILENT:

build:
	-clear
	-dotnet build ./src/ --configuration Debug --verbosity m

test:
	-dotnet test ./src/Tests/Kafka.Connect.UnitTests --configuration Debug --verbosity m || true

publish:
	-rm -rf ./app/bin
	-dotnet publish ./src/Kafka.Connect/Kafka.Connect.csproj --output ./app/bin --configuration Debug --no-build --verbosity m
	-dotnet publish ./src/Plugins/Kafka.Connect.Mongodb/Kafka.Connect.Mongodb.csproj --output ./app/bin/plugins/mongodb --configuration debug --no-build --verbosity m
	-dotnet publish ./src/Plugins/Kafka.Connect.Streamer/Kafka.Connect.Streamer.csproj --output ./app/bin/plugins/streamer --configuration debug --no-build --verbosity m

run:
	-sleep 10
	-clear
	-echo ""
	-echo "=============== KAFKA CONNECT .NET ==============="
	-echo ""
	-sleep 5
	-dotnet ./app/bin/Kafka.Connect.dll --config ./src/Kafka.Connect/appsettings.json
    
launch: build test publish run

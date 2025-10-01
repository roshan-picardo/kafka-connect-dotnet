# Kafka Connect .NET Architecture Diagrams

This directory contains PlantUML diagrams that illustrate the architecture of the Kafka Connect .NET solution.

## Overview

Kafka Connect .NET is a .NET implementation of the Kafka Connect framework, which is used to stream data between Apache Kafka and other systems. The solution includes connectors for various databases (MariaDB, MySQL, Oracle, PostgreSQL, SQL Server, MongoDB, DynamoDB) and provides a plugin architecture for extending functionality.

## Diagrams

### Sequence Diagram (`sequence-diagram.puml`)

The sequence diagram illustrates the flow of operations in the Kafka Connect .NET solution, showing how different components interact over time. It covers:

1. **Initialization Flow**: How the system starts up, creates topics, and initializes connectors
2. **Source Connector Flow**: How data is read from a database and sent to Kafka
3. **Sink Connector Flow**: How data is read from Kafka and written to a database
4. **Changelog/Audit Flow**: How database changes are tracked and processed

### Class Diagram (`class-diagram.puml`)

The class diagram shows the key classes and interfaces in the solution and their relationships. It includes:

1. **Core Interfaces**: IPluginHandler, ISinkHandler, ISourceHandler, etc.
2. **Abstract Classes**: PluginHandler, Strategy, etc.
3. **Concrete Implementations**: MariaDbPluginHandler, InsertStrategy, etc.
4. **Relationships**: Inheritance, composition, and usage relationships between classes

### Component Diagram (`component-diagram.puml`)

The component diagram provides a high-level view of the system architecture, showing the main components and their interactions. It includes:

1. **Main Components**: Leader, Connector, Plugin System, etc.
2. **External Systems**: Kafka, Databases
3. **Plugins**: Database-specific plugins (MariaDB, MySQL, etc.)
4. **Supporting Components**: Strategies, Converters, Processors

## Generating Diagrams

To generate PNG or SVG images from these PlantUML files, you can use:

1. **PlantUML CLI**: 
   ```
   java -jar plantuml.jar sequence-diagram.puml
   ```

2. **Online PlantUML Server**: Upload the .puml files to http://www.plantuml.com/plantuml/

3. **VS Code Extension**: Install the "PlantUML" extension and use Alt+D to preview

## Key Components

### Leader
Orchestrates the overall process, initializes workers, and creates required Kafka topics.

### Worker
Manages connectors and handles the execution of connector tasks. Acts as an intermediary between the Leader and Connectors.

### Connector
Handles the connection to Kafka and processes messages between Kafka and database plugins.

### Plugin System
Manages database-specific plugins and routes operations to the appropriate handlers.

### Message Handler
Processes and transforms messages, handles serialization/deserialization, and applies processors.

### Database Plugins
Implement database-specific operations for different database systems (MariaDB, MySQL, etc.).

### Strategies
Implement specific database operations (Insert, Update, Delete, etc.) and build SQL queries.

### Converters
Handle conversion between different data formats (Avro, JSON, etc.).

### Processors
Apply transformations to messages (field renaming, filtering, type conversion, etc.).

## Architecture Patterns

The solution uses several architectural patterns:

1. **Plugin Architecture**: Database-specific functionality is implemented as plugins
2. **Strategy Pattern**: Different database operations are implemented as strategies
3. **Factory Pattern**: Plugins and strategies are created by factories
4. **Dependency Injection**: Components are loosely coupled through interfaces
5. **Command Pattern**: Operations are encapsulated as commands

## Data Flow

1. **Source Flow**: Database → Plugin → Connector → Kafka
2. **Sink Flow**: Kafka → Connector → Plugin → Database
3. **Changelog Flow**: Database changes → Audit log → Plugin → Connector → Kafka
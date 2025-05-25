# EtlOrchestrator

A flexible, powerful ETL (Extract, Transform, Load) orchestration framework for .NET applications. ETL Orchestrator helps you build, manage, and monitor data pipelines with ease.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET Tests](https://github.com/quimalborch/EtlOrchestrator/actions/workflows/dotnet-tests.yml/badge.svg)](https://github.com/quimalborch/EtlOrchestrator/actions/workflows/dotnet-tests.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/download/dotnet/8.0)
[![GitHub last commit](https://img.shields.io/github/last-commit/quimalborch/EtlOrchestrator)](https://github.com/quimalborch/EtlOrchestrator/commits/main)
[![GitHub repo size](https://img.shields.io/github/repo-size/quimalborch/EtlOrchestrator)](https://github.com/quimalborch/EtlOrchestrator)
[![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/quimalborch/EtlOrchestrator)](https://github.com/quimalborch/EtlOrchestrator)
[![GitHub open issues by-label](https://img.shields.io/github/issues/quimalborch/EtlOrchestrator/bug)](https://github.com/quimalborch/EtlOrchestrator/issues?q=is%3Aissue+is%3Aopen+label%3Abug)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/quimalborch/EtlOrchestrator)

## Features

- **Modular Architecture**: Easily plug in custom connectors for different data sources and destinations
- **Workflow Management**: Define and execute ETL workflows with error handling and retry capabilities
- **Scheduling**: Schedule workflows using cron expressions
- **Monitoring**: Track execution status and performance metrics for all steps
- **Extensibility**: Implement custom transformations and connectors
- **Persistence**: Store workflow definitions, executions, and logs in SQL Server
- **Logging**: Comprehensive logging system with database persistence

## Quick Start

### Prerequisites

- .NET 8.0 or later
- SQL Server (for workflow persistence and logging)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/quimalborch/EtlOrchestrator.git
   cd EtlOrchestrator
   ```

2. Configure connection strings in `appsettings.json`:
   ```json
   {
     "ConnectionStrings": {
       "DefaultConnection": "Server=localhost\\SQLEXPRESS;Database=EtlOrchestrator;Trusted_Connection=True;MultipleActiveResultSets=true;TrustServerCertificate=True"
     }
   }
   ```

3. Install the Entity Framework Core tools (if not already installed):
   ```
   dotnet tool install --global dotnet-ef
   ```

4. Add the Design package to the startup project:
   ```
   cd src/Orchestrator.App/EtlOrchestrator.Orchestrator.App
   dotnet add package Microsoft.EntityFrameworkCore.Design
   ```

5. Run the database migrations from the Infrastructure project, referencing the startup project:
   ```
   cd ../../Infrastructure/EtlOrchestrator.Infrastructure
   dotnet ef migrations add InitialCreate --startup-project ../../Orchestrator.App/EtlOrchestrator.Orchestrator.App
   dotnet ef database update --startup-project ../../Orchestrator.App/EtlOrchestrator.Orchestrator.App
   ```

6. Build and run the application:
   ```
   cd ../../Orchestrator.App/EtlOrchestrator.Orchestrator.App
   dotnet build
   dotnet run
   ```

### Basic Usage

1. **Define a workflow:**

```csharp
// Create a workflow definition
var workflowService = serviceProvider.GetRequiredService<IEtlWorkflowService>();
var workflowJson = @"{
    ""Source"": {
        ""Type"": ""SqlServer"",
        ""ConnectionString"": ""Server=source-server;Database=SourceDb;Trusted_Connection=True"",
        ""SqlQuery"": ""SELECT Id, Name, Email FROM Customers WHERE CreatedDate > @lastRun""
    },
    ""Transform"": {
        ""Type"": ""DataCleaner"",
        ""Rules"": [
            { ""Field"": ""Email"", ""Validations"": [""NotNull"", ""IsEmail""] },
            { ""Field"": ""Name"", ""Transformations"": [""Trim"", ""ToUpper""] }
        ]
    },
    ""Load"": {
        ""Type"": ""SqlServer"",
        ""ConnectionString"": ""Server=dest-server;Database=DestDb;Trusted_Connection=True"",
        ""TargetTable"": ""Customers"",
        ""Strategy"": ""MergeUpsert"",
        ""KeyColumns"": [""Id""]
    }
}";

await workflowService.CreateWorkflowDefinitionAsync("CustomerSync", "Synchronize customer data", workflowJson);
```

2. **Execute a workflow:**

```csharp
// Execute a workflow by ID
var executionResult = await workflowService.ExecuteWorkflowAsync(workflowId);
Console.WriteLine($"Workflow executed with status: {executionResult.Status}");
```

3. **Schedule a workflow:**

```csharp
// Schedule a workflow to run every day at 2:00 AM
await workflowService.ScheduleWorkflowAsync(
    workflowDefinitionId: 1,
    cronExpression: "0 0 2 * * ?",
    timeZone: "UTC",
    description: "Daily customer data sync"
);
```

## Advanced Examples

### Creating a Custom Source Connector

```csharp
public class MyCustomSourceConnector : ISourceConnector
{
    private readonly ILogger<MyCustomSourceConnector> _logger;

    public MyCustomSourceConnector(ILogger<MyCustomSourceConnector> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<Record>> ExtractAsync(Context context)
    {
        _logger.LogInformation("Starting extraction from custom source");
        
        // Your custom extraction logic here
        var records = new List<Record>();
        
        // Create and populate records
        var record = new Record();
        record.SetProperty("Id", 1);
        record.SetProperty("Name", "Example");
        records.Add(record);
        
        return records;
    }
}
```

### Creating a Custom Transformation

```csharp
public class MyCustomTransform : ITransform
{
    private readonly ILogger<MyCustomTransform> _logger;

    public MyCustomTransform(ILogger<MyCustomTransform> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
    {
        _logger.LogInformation("Starting custom transformation");
        
        var transformedRecords = new List<Record>();
        foreach (var record in records)
        {
            var transformed = new Record();
            // Apply your custom transformations
            foreach (var property in record.GetProperties())
            {
                transformed.SetProperty(property.Key, property.Value);
            }
            
            // Add custom properties
            transformed.SetProperty("TransformedAt", DateTime.UtcNow);
            
            transformedRecords.Add(transformed);
        }
        
        return transformedRecords;
    }
}
```

### Monitoring Workflow Executions

```csharp
// Get all executions of a specific workflow
var executions = await workflowService.GetWorkflowExecutionsByDefinitionIdAsync(workflowDefinitionId);

// Get detailed execution information including steps
var executionDetail = await workflowService.GetWorkflowExecutionByIdAsync(executionId);

// Display execution details
Console.WriteLine($"Workflow: {executionDetail.WorkflowId}");
Console.WriteLine($"Status: {executionDetail.Status}");
Console.WriteLine($"Started: {executionDetail.StartTime}");
Console.WriteLine($"Ended: {executionDetail.EndTime}");

// Show step details
foreach (var step in executionDetail.Steps)
{
    Console.WriteLine($"- Step: {step.StepName} ({step.StepType})");
    Console.WriteLine($"  Status: {step.Status}");
    Console.WriteLine($"  Records Processed: {step.RecordsProcessed}");
    Console.WriteLine($"  Duration: {(step.EndTime - step.StartTime).TotalSeconds}s");
}
```

## Architecture

The EtlOrchestrator is built on a modular architecture:

- **Core Layer**: Contains interfaces and models
- **Infrastructure Layer**: Implements connectors, workflows, persistence, and scheduling
- **API Layer**: Provides HTTP endpoints for managing workflows (optional)

## Testing

ETL Orchestrator follows best practices for testing, with a comprehensive suite of unit tests ensuring code quality and reliability.

### Test Projects Structure

The solution includes three test projects, each targeting a specific layer of the application:

- **Core.Tests**: Tests for the core domain models (`Record`, `Context`, etc.)
- **Infrastructure.Tests**: Tests for the infrastructure services, including workflow management and repositories
- **Orchestrator.App.Tests**: Tests for the API controllers and endpoints

### Running Tests

You can run all tests using the .NET CLI:

```bash
dotnet test
```

Or run specific test projects:

```bash
dotnet test tests/Core.Tests/EtlOrchestrator.Core.Tests/EtlOrchestrator.Core.Tests.csproj
```

### Test Coverage

Test coverage is automatically calculated during CI/CD builds using the Coverlet library and ReportGenerator. To generate a coverage report locally:

```bash
dotnet test --collect:"XPlat Code Coverage"
dotnet tool install -g dotnet-reportgenerator-globaltool
reportgenerator -reports:"**/coverage.cobertura.xml" -targetdir:"coveragereport" -reporttypes:Html
```

Then open `coveragereport/index.html` in your browser.

### Testing Approach

Our testing strategy follows industry best practices:

1. **Unit Tests**: Focus on testing individual components in isolation
2. **Mocking**: Use Moq to create mock dependencies
3. **Arrange-Act-Assert**: Structure tests with clear setup, action, and verification phases
4. **Parameterized Tests**: Use xUnit's theory tests for testing multiple data scenarios
5. **Integration with CI/CD**: Automatic test execution on every push or pull request

### Continuous Integration

ETL Orchestrator uses GitHub Actions to automatically run tests on every push and pull request. The workflow configuration can be found in `.github/workflows/dotnet-tests.yml`.

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
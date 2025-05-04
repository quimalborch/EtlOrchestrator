using System;
using System.Collections.Generic;
using Xunit;
using EtlOrchestrator.Core;

namespace EtlOrchestrator.Core.Tests
{
    public class RecordTests
    {
        [Fact]
        public void Record_Creation_GeneratesUniqueId()
        {
            // Arrange & Act
            var record1 = new Record();
            var record2 = new Record();

            // Assert
            Assert.NotEqual(Guid.Empty, record1.Id);
            Assert.NotEqual(Guid.Empty, record2.Id);
            Assert.NotEqual(record1.Id, record2.Id);
        }

        [Fact]
        public void Record_SetAndGetProperty_WorksCorrectly()
        {
            // Arrange
            var record = new Record();
            string propertyName = "TestProperty";
            string propertyValue = "TestValue";

            // Act
            record.SetProperty(propertyName, propertyValue);
            var result = record[propertyName];
            bool tryGetSuccess = record.TryGetProperty(propertyName, out var outValue);

            // Assert
            Assert.Equal(propertyValue, result);
            Assert.True(tryGetSuccess);
            Assert.Equal(propertyValue, outValue);
        }

        [Fact]
        public void Record_GetProperties_ReturnsAllProperties()
        {
            // Arrange
            var record = new Record();
            record.SetProperty("Prop1", "Value1");
            record.SetProperty("Prop2", 42);
            record.SetProperty("Prop3", true);

            // Act
            var properties = record.GetProperties();

            // Assert
            Assert.Equal(3, properties.Count);
            Assert.Equal("Value1", properties["Prop1"]);
            Assert.Equal(42, properties["Prop2"]);
            Assert.Equal(true, properties["Prop3"]);
        }

        [Fact]
        public void Record_IndexerAccess_WorksCorrectly()
        {
            // Arrange
            var record = new Record();

            // Act
            record["Name"] = "Test";
            record["Age"] = 30;

            // Assert
            Assert.Equal("Test", record["Name"]);
            Assert.Equal(30, record["Age"]);
            Assert.Null(record["NonExistent"]);
        }

        [Fact]
        public void Record_DynamicAccess_WorksCorrectly()
        {
            // Arrange
            dynamic record = new Record();

            // Act
            record.Name = "Dynamic Test";
            record.Value = 123;

            // Assert
            Assert.Equal("Dynamic Test", record.Name);
            Assert.Equal(123, record.Value);
        }

        [Fact]
        public void Record_Metadata_CanBeModified()
        {
            // Arrange
            var record = new Record();

            // Act
            record.Metadata["Source"] = "Database";
            record.Metadata["Timestamp"] = DateTime.UtcNow;

            // Assert
            Assert.Equal("Database", record.Metadata["Source"]);
            Assert.True(record.Metadata.ContainsKey("Timestamp"));
        }
    }

    public class ContextTests
    {
        [Fact]
        public void Context_Creation_HasEmptyParameters()
        {
            // Arrange & Act
            var context = new Context();

            // Assert
            Assert.NotNull(context.Parameters);
            Assert.Empty(context.Parameters);
        }

        [Fact]
        public void Context_SetAndGetParameter_WorksCorrectly()
        {
            // Arrange
            var context = new Context();
            string paramName = "TestParam";
            string paramValue = "TestValue";

            // Act
            context.SetParameter(paramName, paramValue);
            var result = context.GetParameter<string>(paramName);

            // Assert
            Assert.Equal(paramValue, result);
        }

        [Fact]
        public void Context_GetParameter_WithDefaultValue_ReturnsDefaultWhenNotFound()
        {
            // Arrange
            var context = new Context();
            string defaultValue = "DefaultValue";

            // Act
            var result = context.GetParameter<string>("NonExistentParam", defaultValue);

            // Assert
            Assert.Equal(defaultValue, result);
        }

        [Fact]
        public void Context_TryGetParameter_ReturnsFalseWhenNotFound()
        {
            // Arrange
            var context = new Context();

            // Act
            bool success = context.TryGetParameter<string>("NonExistentParam", out var value);

            // Assert
            Assert.False(success);
            Assert.Null(value);
        }

        [Fact]
        public void Context_TryGetParameter_ReturnsTrueWhenFound()
        {
            // Arrange
            var context = new Context();
            string paramName = "TestParam";
            string paramValue = "TestValue";
            context.SetParameter(paramName, paramValue);

            // Act
            bool success = context.TryGetParameter<string>(paramName, out var value);

            // Assert
            Assert.True(success);
            Assert.Equal(paramValue, value);
        }

        [Fact]
        public void Context_HasUniqueId()
        {
            // Arrange & Act
            var context1 = new Context();
            var context2 = new Context();

            // Assert
            Assert.NotEqual(Guid.Empty, context1.Id);
            Assert.NotEqual(Guid.Empty, context2.Id);
            Assert.NotEqual(context1.Id, context2.Id);
        }

        [Fact]
        public void Context_JobProperties_CanBeSet()
        {
            // Arrange
            var context = new Context();
            string jobName = "TestJob";
            string executionId = "123456";
            DateTime startTime = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc);

            // Act
            context.JobName = jobName;
            context.ExecutionId = executionId;
            context.StartTime = startTime;

            // Assert
            Assert.Equal(jobName, context.JobName);
            Assert.Equal(executionId, context.ExecutionId);
            Assert.Equal(startTime, context.StartTime);
        }
    }
}
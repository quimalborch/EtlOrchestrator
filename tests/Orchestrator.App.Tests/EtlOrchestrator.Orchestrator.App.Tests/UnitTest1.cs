using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlOrchestrator.Infrastructure.Services;
using EtlOrchestrator.Orchestrator.App.Controllers;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;
using System.Reflection;
using Newtonsoft.Json;

namespace EtlOrchestrator.Orchestrator.App.Tests
{
    public class WorkflowControllerTests
    {
        private readonly Mock<IEtlWorkflowService> _mockWorkflowService;
        private readonly WorkflowController _controller;

        public WorkflowControllerTests()
        {
            _mockWorkflowService = new Mock<IEtlWorkflowService>();
            _controller = new WorkflowController(_mockWorkflowService.Object);
        }

        [Fact]
        public async Task GetWorkflowDefinitions_ReturnsOkResult_WithListOfDefinitions()
        {
            // Arrange
            var expectedDefinitions = new List<WorkflowDefinition>
            {
                new WorkflowDefinition { Id = 1, Name = "Workflow 1" },
                new WorkflowDefinition { Id = 2, Name = "Workflow 2" }
            };

            _mockWorkflowService.Setup(service => service.GetAllWorkflowDefinitionsAsync())
                .ReturnsAsync(expectedDefinitions);

            // Act
            var result = await _controller.GetWorkflowDefinitions();

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result.Result);
            var returnedDefinitions = Assert.IsAssignableFrom<IEnumerable<WorkflowDefinition>>(okResult.Value);
            Assert.Equal(2, ((List<WorkflowDefinition>)returnedDefinitions).Count);
        }

        [Fact]
        public async Task GetWorkflowDefinition_WithValidId_ReturnsOkResult()
        {
            // Arrange
            var expectedDefinition = new WorkflowDefinition { Id = 1, Name = "Test Workflow" };
            
            _mockWorkflowService.Setup(service => service.GetWorkflowDefinitionByIdAsync(1))
                .ReturnsAsync(expectedDefinition);

            // Act
            var result = await _controller.GetWorkflowDefinition(1);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result.Result);
            var returnedDefinition = Assert.IsType<WorkflowDefinition>(okResult.Value);
            Assert.Equal(1, returnedDefinition.Id);
            Assert.Equal("Test Workflow", returnedDefinition.Name);
        }

        [Fact]
        public async Task GetWorkflowDefinition_WithInvalidId_ReturnsNotFound()
        {
            // Arrange
            _mockWorkflowService.Setup(service => service.GetWorkflowDefinitionByIdAsync(999))
                .ReturnsAsync((WorkflowDefinition)null);

            // Act
            var result = await _controller.GetWorkflowDefinition(999);

            // Assert
            Assert.IsType<NotFoundResult>(result.Result);
        }

        [Fact]
        public async Task CreateWorkflowDefinition_WithValidData_ReturnsCreatedResult()
        {
            // Arrange
            var request = new CreateWorkflowDefinitionRequest
            {
                Name = "New Workflow",
                Description = "Test Description",
                ConfigurationJson = "{\"steps\": []}"
            };

            var createdDefinition = new WorkflowDefinition
            {
                Id = 1,
                Name = "New Workflow",
                Description = "Test Description",
                ConfigurationJson = "{\"steps\": []}"
            };

            _mockWorkflowService.Setup(service => service.CreateWorkflowDefinitionAsync(
                    request.Name, request.Description, request.ConfigurationJson))
                .ReturnsAsync(createdDefinition);

            // Act
            var result = await _controller.CreateWorkflowDefinition(request);

            // Assert
            var createdAtActionResult = Assert.IsType<CreatedAtActionResult>(result.Result);
            Assert.Equal(nameof(WorkflowController.GetWorkflowDefinition), createdAtActionResult.ActionName);
            Assert.Equal(1, createdAtActionResult.RouteValues["id"]);
            
            var returnedDefinition = Assert.IsType<WorkflowDefinition>(createdAtActionResult.Value);
            Assert.Equal(1, returnedDefinition.Id);
            Assert.Equal("New Workflow", returnedDefinition.Name);
        }

        [Fact]
        public async Task ExecuteWorkflow_WithValidId_ReturnsOkResult()
        {
            // Arrange
            var request = new ExecuteWorkflowRequest { InputDataJson = "{\"param\": \"value\"}" };
            var execution = new WorkflowExecution { Id = 1, WorkflowDefinitionId = 1, Status = "Completed" };

            _mockWorkflowService.Setup(service => service.ExecuteWorkflowAsync(1, request.InputDataJson))
                .ReturnsAsync(execution);

            // Act
            var result = await _controller.ExecuteWorkflow(1, request);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result.Result);
            var returnedExecution = Assert.IsType<WorkflowExecution>(okResult.Value);
            Assert.Equal(1, returnedExecution.Id);
            Assert.Equal("Completed", returnedExecution.Status);
        }

        [Fact]
        public async Task ExecuteWorkflow_WithInvalidId_ReturnsNotFound()
        {
            // Arrange
            var request = new ExecuteWorkflowRequest { InputDataJson = "{\"param\": \"value\"}" };

            _mockWorkflowService.Setup(service => service.ExecuteWorkflowAsync(999, request.InputDataJson))
                .ThrowsAsync(new KeyNotFoundException());

            // Act
            var result = await _controller.ExecuteWorkflow(999, request);

            // Assert
            Assert.IsType<NotFoundResult>(result.Result);
        }

        [Fact]
        public async Task GetWorkflowSchedules_ReturnsOkResult_WithListOfSchedules()
        {
            // Arrange
            var expectedSchedules = new List<WorkflowSchedule>
            {
                new WorkflowSchedule { Id = 1, WorkflowDefinitionId = 1, CronExpression = "0 0 * * *" },
                new WorkflowSchedule { Id = 2, WorkflowDefinitionId = 2, CronExpression = "0 12 * * *" }
            };

            _mockWorkflowService.Setup(service => service.GetAllWorkflowSchedulesAsync())
                .ReturnsAsync(expectedSchedules);

            // Act
            var result = await _controller.GetWorkflowSchedules();

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result.Result);
            var returnedSchedules = Assert.IsAssignableFrom<IEnumerable<WorkflowSchedule>>(okResult.Value);
            Assert.Equal(2, ((List<WorkflowSchedule>)returnedSchedules).Count);
        }

        [Fact]
        public async Task SetWorkflowScheduleStatus_WithValidId_ReturnsOkResult()
        {
            // Arrange
            var request = new SetWorkflowScheduleStatusRequest { Enabled = true };

            _mockWorkflowService.Setup(service => service.SetWorkflowScheduleStatusAsync(1, true))
                .ReturnsAsync(true);

            // Act
            var result = await _controller.SetWorkflowScheduleStatus(1, request);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            
            // Convertir el objeto anónimo a un objeto dinámico mediante serialización
            var json = JsonConvert.SerializeObject(okResult.Value);
            var deserializedObject = JsonConvert.DeserializeObject<Dictionary<string, bool>>(json);
            
            // Verificar que contiene la propiedad Success y que su valor es true
            Assert.True(deserializedObject.ContainsKey("Success"));
            Assert.True(deserializedObject["Success"]);
        }
    }
}
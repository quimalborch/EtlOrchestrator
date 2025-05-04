using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlOrchestrator.Infrastructure.Services;
using Microsoft.AspNetCore.Mvc;

namespace EtlOrchestrator.Orchestrator.App.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class WorkflowController : ControllerBase
    {
        private readonly IEtlWorkflowService _workflowService;

        public WorkflowController(IEtlWorkflowService workflowService)
        {
            _workflowService = workflowService ?? throw new ArgumentNullException(nameof(workflowService));
        }

        // GET: api/workflow/definitions
        [HttpGet("definitions")]
        public async Task<ActionResult<IEnumerable<WorkflowDefinition>>> GetWorkflowDefinitions()
        {
            var definitions = await _workflowService.GetAllWorkflowDefinitionsAsync();
            return Ok(definitions);
        }

        // GET: api/workflow/definitions/{id}
        [HttpGet("definitions/{id}")]
        public async Task<ActionResult<WorkflowDefinition>> GetWorkflowDefinition(int id)
        {
            var definition = await _workflowService.GetWorkflowDefinitionByIdAsync(id);
            if (definition == null)
            {
                return NotFound();
            }
            return Ok(definition);
        }

        // POST: api/workflow/definitions
        [HttpPost("definitions")]
        public async Task<ActionResult<WorkflowDefinition>> CreateWorkflowDefinition([FromBody] CreateWorkflowDefinitionRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var definition = await _workflowService.CreateWorkflowDefinitionAsync(
                request.Name, 
                request.Description, 
                request.ConfigurationJson);

            return CreatedAtAction(
                nameof(GetWorkflowDefinition), 
                new { id = definition.Id }, 
                definition);
        }

        // PUT: api/workflow/definitions/{id}
        [HttpPut("definitions/{id}")]
        public async Task<IActionResult> UpdateWorkflowDefinition(int id, [FromBody] UpdateWorkflowDefinitionRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                var definition = await _workflowService.UpdateWorkflowDefinitionAsync(
                    id,
                    request.Description, 
                    request.ConfigurationJson);
                
                return Ok(definition);
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
        }

        // PATCH: api/workflow/definitions/{id}/status
        [HttpPatch("definitions/{id}/status")]
        public async Task<IActionResult> SetWorkflowDefinitionStatus(int id, [FromBody] SetWorkflowStatusRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                var result = await _workflowService.SetWorkflowDefinitionStatusAsync(id, request.IsActive);
                return Ok(new { Success = result });
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
        }

        // GET: api/workflow/executions
        [HttpGet("executions")]
        public async Task<ActionResult<IEnumerable<WorkflowExecution>>> GetWorkflowExecutions()
        {
            var executions = await _workflowService.GetAllWorkflowExecutionsAsync();
            return Ok(executions);
        }

        // GET: api/workflow/executions/{id}
        [HttpGet("executions/{id}")]
        public async Task<ActionResult<WorkflowExecution>> GetWorkflowExecution(int id)
        {
            var execution = await _workflowService.GetWorkflowExecutionByIdAsync(id);
            if (execution == null)
            {
                return NotFound();
            }
            return Ok(execution);
        }

        // POST: api/workflow/definitions/{id}/execute
        [HttpPost("definitions/{id}/execute")]
        public async Task<ActionResult<WorkflowExecution>> ExecuteWorkflow(int id, [FromBody] ExecuteWorkflowRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                var execution = await _workflowService.ExecuteWorkflowAsync(id, request.InputDataJson);
                return Ok(execution);
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
            catch (Exception ex)
            {
                return BadRequest(new { Error = ex.Message });
            }
        }

        // GET: api/workflow/executions/{id}/steps
        [HttpGet("executions/{id}/steps")]
        public async Task<ActionResult<IEnumerable<WorkflowExecutionStep>>> GetWorkflowExecutionSteps(int id)
        {
            try
            {
                var steps = await _workflowService.GetWorkflowExecutionStepsAsync(id);
                return Ok(steps);
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
        }

        // GET: api/workflow/schedules
        [HttpGet("schedules")]
        public async Task<ActionResult<IEnumerable<WorkflowSchedule>>> GetWorkflowSchedules()
        {
            var schedules = await _workflowService.GetAllWorkflowSchedulesAsync();
            return Ok(schedules);
        }

        // GET: api/workflow/schedules/{id}
        [HttpGet("schedules/{id}")]
        public async Task<ActionResult<WorkflowSchedule>> GetWorkflowSchedule(int id)
        {
            var schedule = await _workflowService.GetWorkflowScheduleByIdAsync(id);
            if (schedule == null)
            {
                return NotFound();
            }
            return Ok(schedule);
        }

        // POST: api/workflow/definitions/{id}/schedule
        [HttpPost("definitions/{id}/schedule")]
        public async Task<ActionResult<WorkflowSchedule>> ScheduleWorkflow(int id, [FromBody] CreateWorkflowScheduleRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                var schedule = await _workflowService.CreateWorkflowScheduleAsync(
                    id,
                    request.CronExpression,
                    request.Description,
                    request.InputDataJson);

                return CreatedAtAction(
                    nameof(GetWorkflowSchedule),
                    new { id = schedule.Id },
                    schedule);
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
            catch (Exception ex)
            {
                return BadRequest(new { Error = ex.Message });
            }
        }

        // PATCH: api/workflow/schedules/{id}/status
        [HttpPatch("schedules/{id}/status")]
        public async Task<IActionResult> SetWorkflowScheduleStatus(int id, [FromBody] SetWorkflowScheduleStatusRequest request)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                var result = await _workflowService.SetWorkflowScheduleStatusAsync(id, request.Enabled);
                return Ok(new { Success = result });
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
        }

        // DELETE: api/workflow/schedules/{id}
        [HttpDelete("schedules/{id}")]
        public async Task<IActionResult> DeleteWorkflowSchedule(int id)
        {
            try
            {
                var result = await _workflowService.DeleteWorkflowScheduleAsync(id);
                return Ok(new { Success = result });
            }
            catch (KeyNotFoundException)
            {
                return NotFound();
            }
        }
    }

    public class CreateWorkflowDefinitionRequest
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string ConfigurationJson { get; set; }
    }

    public class UpdateWorkflowDefinitionRequest
    {
        public string Description { get; set; }
        public string ConfigurationJson { get; set; }
    }

    public class ExecuteWorkflowRequest
    {
        public string InputDataJson { get; set; }
    }

    public class SetWorkflowStatusRequest
    {
        public bool IsActive { get; set; }
    }

    public class CreateWorkflowScheduleRequest
    {
        public string CronExpression { get; set; }
        public string Description { get; set; }
        public string InputDataJson { get; set; }
    }

    public class SetWorkflowScheduleStatusRequest
    {
        public bool Enabled { get; set; }
    }
} 
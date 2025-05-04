using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlWorkflowRepo = EtlOrchestrator.Infrastructure.Persistence.Repositories;
using EtlOrchestrator.Infrastructure.Scheduler;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using WorkflowCore.Interface;

namespace EtlOrchestrator.Infrastructure.Services
{
    /// <summary>
    /// Implementación del servicio de gestión de flujos de trabajo ETL
    /// </summary>
    public class EtlWorkflowService : IEtlWorkflowService
    {
        private readonly EtlWorkflowRepo.IWorkflowRepository _repository;
        private readonly IWorkflowHost _workflowHost;
        private readonly CronWorkflowScheduler _scheduler;
        private readonly ILogger<EtlWorkflowService> _logger;

        public EtlWorkflowService(
            EtlWorkflowRepo.IWorkflowRepository repository,
            IWorkflowHost workflowHost,
            CronWorkflowScheduler scheduler,
            ILogger<EtlWorkflowService> logger)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _workflowHost = workflowHost ?? throw new ArgumentNullException(nameof(workflowHost));
            _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        #region Workflow Definition Management

        public async Task<IEnumerable<WorkflowDefinition>> GetAllWorkflowDefinitionsAsync()
        {
            return await _repository.GetAllWorkflowDefinitionsAsync();
        }

        public async Task<WorkflowDefinition> GetWorkflowDefinitionByIdAsync(int id)
        {
            return await _repository.GetWorkflowDefinitionByIdAsync(id);
        }

        public async Task<WorkflowDefinition> CreateWorkflowDefinitionAsync(string name, string description, string configurationJson)
        {
            try
            {
                _logger.LogInformation("Creando nueva definición de workflow: {Name}", name);
                
                // Validar que el JSON sea válido
                if (!string.IsNullOrEmpty(configurationJson))
                {
                    JsonConvert.DeserializeObject(configurationJson);
                }

                var definition = new WorkflowDefinition
                {
                    Name = name,
                    Description = description,
                    ConfigurationJson = configurationJson,
                    Version = 1,
                    Created = DateTime.UtcNow,
                    LastModified = DateTime.UtcNow,
                    IsActive = true
                };

                return await _repository.CreateWorkflowDefinitionAsync(definition);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear definición de workflow");
                throw;
            }
        }

        public async Task<WorkflowDefinition> UpdateWorkflowDefinitionAsync(int id, string description, string configurationJson)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(id);
                if (definition == null)
                {
                    throw new KeyNotFoundException($"No se encontró la definición de workflow con ID {id}");
                }

                // Validar que el JSON sea válido
                if (!string.IsNullOrEmpty(configurationJson))
                {
                    JsonConvert.DeserializeObject(configurationJson);
                }

                // Actualizar propiedades
                definition.Description = description;
                definition.ConfigurationJson = configurationJson;
                definition.LastModified = DateTime.UtcNow;
                definition.Version += 1; // Incrementar versión

                return await _repository.UpdateWorkflowDefinitionAsync(definition);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar definición de workflow con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> SetWorkflowDefinitionStatusAsync(int id, bool isActive)
        {
            try
            {
                return await _repository.SetWorkflowDefinitionStatusAsync(id, isActive);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cambiar el estado de la definición de workflow con ID {Id}", id);
                throw;
            }
        }

        #endregion

        #region Workflow Execution Management

        public async Task<IEnumerable<WorkflowExecution>> GetAllWorkflowExecutionsAsync()
        {
            return await _repository.GetAllWorkflowExecutionsAsync();
        }

        public async Task<WorkflowExecution> GetWorkflowExecutionByIdAsync(int id)
        {
            return await _repository.GetWorkflowExecutionByIdAsync(id);
        }

        public async Task<WorkflowExecution> ExecuteWorkflowAsync(int workflowDefinitionId, string inputDataJson)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(workflowDefinitionId);
                if (definition == null)
                {
                    throw new KeyNotFoundException($"No se encontró la definición de workflow con ID {workflowDefinitionId}");
                }

                if (!definition.IsActive)
                {
                    throw new InvalidOperationException("No se puede ejecutar un workflow inactivo");
                }

                // Crear registro de ejecución
                var execution = new WorkflowExecution
                {
                    WorkflowDefinitionId = workflowDefinitionId,
                    StartTime = DateTime.UtcNow,
                    Status = "Iniciando",
                    InputDataJson = inputDataJson
                };

                execution = await _repository.CreateWorkflowExecutionAsync(execution);

                // Iniciar el workflow en WorkflowCore
                var data = new Workflow.EtlWorkflowData
                {
                    ExecutionId = execution.Id,
                    Configuration = definition.ConfigurationJson,
                    InputData = inputDataJson,
                    Context = new Core.Context
                    {
                        JobName = definition.Name,
                        ExecutionId = execution.Id.ToString(),
                        StartTime = DateTime.UtcNow
                    },
                    StartTime = DateTime.UtcNow,
                    Success = true
                };

                var workflowId = definition.Name;
                var instanceId = await _workflowHost.StartWorkflow(workflowId, data);

                // Actualizar la ejecución con los IDs de WorkflowCore
                execution.WorkflowId = workflowId;
                execution.InstanceId = instanceId;
                execution.Status = "En ejecución";
                
                await _repository.UpdateWorkflowExecutionAsync(execution);

                return execution;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar workflow con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<IEnumerable<WorkflowExecutionStep>> GetWorkflowExecutionStepsAsync(int executionId)
        {
            try
            {
                var execution = await _repository.GetWorkflowExecutionByIdAsync(executionId);
                if (execution == null)
                {
                    throw new KeyNotFoundException($"No se encontró la ejecución con ID {executionId}");
                }

                return await _repository.GetWorkflowExecutionStepsByExecutionIdAsync(executionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener los pasos de ejecución para la ejecución con ID {Id}", executionId);
                throw;
            }
        }

        #endregion

        #region Workflow Schedule Management

        public async Task<IEnumerable<WorkflowSchedule>> GetAllWorkflowSchedulesAsync()
        {
            return await _repository.GetAllWorkflowSchedulesAsync();
        }

        public async Task<WorkflowSchedule> GetWorkflowScheduleByIdAsync(int id)
        {
            return await _repository.GetWorkflowScheduleByIdAsync(id);
        }

        public async Task<WorkflowSchedule> CreateWorkflowScheduleAsync(int workflowDefinitionId, string cronExpression, string description, string inputDataJson)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(workflowDefinitionId);
                if (definition == null)
                {
                    throw new KeyNotFoundException($"No se encontró la definición de workflow con ID {workflowDefinitionId}");
                }

                if (!definition.IsActive)
                {
                    throw new InvalidOperationException("No se puede programar un workflow inactivo");
                }

                // Validar expresión cron
                if (string.IsNullOrEmpty(cronExpression))
                {
                    throw new ArgumentException("La expresión cron no puede estar vacía");
                }

                // Crear una nueva programación
                var schedule = new WorkflowSchedule
                {
                    WorkflowDefinitionId = workflowDefinitionId,
                    WorkflowId = definition.Name,
                    CronExpression = cronExpression,
                    TimeZone = "UTC", // Usar UTC como zona horaria por defecto
                    Description = description,
                    InputDataJson = inputDataJson,
                    Created = DateTime.UtcNow,
                    LastModified = DateTime.UtcNow,
                    Enabled = true
                };

                // Guardar en base de datos
                schedule = await _repository.CreateWorkflowScheduleAsync(schedule);

                // Programar con Hangfire
                var jobId = _scheduler.ScheduleWorkflow(
                    workflowDefinitionId, 
                    definition.Name,
                    schedule.Id,
                    cronExpression,
                    inputDataJson);

                // Actualizar con el ID del trabajo de Hangfire
                schedule.JobId = jobId;
                schedule.NextExecution = _scheduler.GetNextExecutionTime(cronExpression);
                await _repository.UpdateWorkflowScheduleAsync(schedule);

                return schedule;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear programación para workflow con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<bool> SetWorkflowScheduleStatusAsync(int id, bool enabled)
        {
            try
            {
                var schedule = await _repository.GetWorkflowScheduleByIdAsync(id);
                if (schedule == null)
                {
                    throw new KeyNotFoundException($"No se encontró la programación con ID {id}");
                }

                if (enabled)
                {
                    // Habilitar programación
                    if (!string.IsNullOrEmpty(schedule.JobId))
                    {
                        _scheduler.DeleteJob(schedule.JobId);
                    }

                    var definition = await _repository.GetWorkflowDefinitionByIdAsync(schedule.WorkflowDefinitionId);
                    var jobId = _scheduler.ScheduleWorkflow(
                        schedule.WorkflowDefinitionId,
                        schedule.WorkflowId,
                        schedule.Id,
                        schedule.CronExpression,
                        schedule.InputDataJson);

                    schedule.JobId = jobId;
                    schedule.NextExecution = _scheduler.GetNextExecutionTime(schedule.CronExpression);
                    await _repository.UpdateWorkflowScheduleAsync(schedule);
                }
                else
                {
                    // Deshabilitar programación
                    if (!string.IsNullOrEmpty(schedule.JobId))
                    {
                        _scheduler.DeleteJob(schedule.JobId);
                        schedule.JobId = null;
                        schedule.NextExecution = null;
                        await _repository.UpdateWorkflowScheduleAsync(schedule);
                    }
                }

                return await _repository.SetWorkflowScheduleStatusAsync(id, enabled);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cambiar el estado de la programación con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> DeleteWorkflowScheduleAsync(int id)
        {
            try
            {
                var schedule = await _repository.GetWorkflowScheduleByIdAsync(id);
                if (schedule == null)
                {
                    throw new KeyNotFoundException($"No se encontró la programación con ID {id}");
                }

                // Eliminar trabajo de Hangfire
                if (!string.IsNullOrEmpty(schedule.JobId))
                {
                    _scheduler.DeleteJob(schedule.JobId);
                }

                return await _repository.DeleteWorkflowScheduleAsync(id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al eliminar programación con ID {Id}", id);
                throw;
            }
        }

        public async Task UpdateScheduleExecutionInfoAsync(int scheduleId, DateTime lastExecution)
        {
            try
            {
                var schedule = await _repository.GetWorkflowScheduleByIdAsync(scheduleId);
                if (schedule == null)
                {
                    throw new KeyNotFoundException($"No se encontró la programación con ID {scheduleId}");
                }

                // Calcular la próxima ejecución
                var nextExecution = _scheduler.GetNextExecutionTime(schedule.CronExpression);

                // Actualizar metadatos de la programación
                await _repository.UpdateWorkflowScheduleExecutionMetadataAsync(
                    scheduleId,
                    lastExecution,
                    nextExecution);

                _logger.LogInformation("Actualizada información de ejecución para la programación {ScheduleId}, última ejecución: {LastExecution}, próxima ejecución: {NextExecution}",
                    scheduleId, lastExecution, nextExecution);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar información de ejecución para la programación {ScheduleId}", scheduleId);
                throw;
            }
        }

        #endregion

        #region Workflow Logs

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByWorkflowIdAsync(string workflowId)
        {
            return await _repository.GetWorkflowLogsByWorkflowIdAsync(workflowId);
        }

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByInstanceIdAsync(string instanceId)
        {
            return await _repository.GetWorkflowLogsByInstanceIdAsync(instanceId);
        }

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByDateRangeAsync(DateTime startDate, DateTime endDate)
        {
            return await _repository.GetWorkflowLogsByDateRangeAsync(startDate, endDate);
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Valida que el JSON de configuración del flujo de trabajo sea válido
        /// </summary>
        private void ValidateConfigurationJson(string configurationJson)
        {
            try
            {
                // Verificar que sea un JSON válido
                var configObj = JsonConvert.DeserializeObject(configurationJson);
                if (configObj == null)
                {
                    throw new ArgumentException("La configuración del flujo de trabajo no es un JSON válido");
                }
            }
            catch (JsonException ex)
            {
                throw new ArgumentException("La configuración del flujo de trabajo no es un JSON válido", ex);
            }
        }

        /// <summary>
        /// Prepara los datos de entrada para el flujo de trabajo
        /// </summary>
        private object PrepareWorkflowData(WorkflowDefinition definition, string inputDataJson, int executionId)
        {
            try
            {
                // Crear un objeto dinámico con los datos de entrada
                dynamic data = new
                {
                    WorkflowId = definition.Name,
                    WorkflowVersion = definition.Version,
                    ExecutionId = executionId,
                    InputData = inputDataJson,
                    StartTime = DateTime.UtcNow
                };

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al preparar los datos de entrada para el flujo de trabajo {Name}", definition.Name);
                throw;
            }
        }

        #endregion
    }
} 
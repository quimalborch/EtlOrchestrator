using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlOrchestrator.Infrastructure.Persistence.Repositories;
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
        private readonly IWorkflowRepository _repository;
        private readonly IWorkflowHost _workflowHost;
        private readonly CronWorkflowScheduler _scheduler;
        private readonly ILogger<EtlWorkflowService> _logger;

        public EtlWorkflowService(
            IWorkflowRepository repository,
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
                // Validar que el JSON de configuración sea válido
                ValidateConfigurationJson(configurationJson);

                var workflowDefinition = new WorkflowDefinition
                {
                    Name = name,
                    Description = description,
                    ConfigurationJson = configurationJson,
                    IsActive = true
                };

                return await _repository.CreateWorkflowDefinitionAsync(workflowDefinition);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear la definición de flujo de trabajo {Name}", name);
                throw;
            }
        }

        public async Task<WorkflowDefinition> UpdateWorkflowDefinitionAsync(int id, string description, string configurationJson)
        {
            try
            {
                var existingDefinition = await _repository.GetWorkflowDefinitionByIdAsync(id);
                if (existingDefinition == null)
                {
                    throw new ArgumentException($"No se encontró la definición de flujo de trabajo con ID {id}");
                }

                // Validar que el JSON de configuración sea válido
                ValidateConfigurationJson(configurationJson);

                existingDefinition.Description = description;
                existingDefinition.ConfigurationJson = configurationJson;

                return await _repository.UpdateWorkflowDefinitionAsync(existingDefinition);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la definición de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> SetWorkflowDefinitionStatusAsync(int id, bool isActive)
        {
            return await _repository.SetWorkflowDefinitionStatusAsync(id, isActive);
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

        public async Task<IEnumerable<WorkflowExecution>> GetWorkflowExecutionsByDefinitionIdAsync(int workflowDefinitionId)
        {
            return await _repository.GetWorkflowExecutionsByDefinitionIdAsync(workflowDefinitionId);
        }

        public async Task<WorkflowExecution> ExecuteWorkflowAsync(int workflowDefinitionId, string inputDataJson = null)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(workflowDefinitionId);
                if (definition == null)
                {
                    throw new ArgumentException($"No se encontró la definición de flujo de trabajo con ID {workflowDefinitionId}");
                }

                if (!definition.IsActive)
                {
                    throw new InvalidOperationException($"La definición de flujo de trabajo con ID {workflowDefinitionId} no está activa");
                }

                // Crear registro de ejecución en la base de datos
                var execution = new WorkflowExecution
                {
                    WorkflowDefinitionId = workflowDefinitionId,
                    WorkflowId = definition.Name,
                    InputDataJson = inputDataJson,
                    Status = "Iniciando"
                };

                execution = await _repository.CreateWorkflowExecutionAsync(execution);

                try
                {
                    // Preparar datos de entrada
                    var dataObj = PrepareWorkflowData(definition, inputDataJson, execution.Id);

                    // Generar un instanceId único
                    var instanceId = Guid.NewGuid().ToString();
                    execution.InstanceId = instanceId;
                    await _repository.UpdateWorkflowExecutionAsync(execution);

                    // Registrar inicio de la ejecución
                    _logger.LogInformation("Iniciando ejecución de flujo de trabajo {Name} con instancia {InstanceId}",
                        definition.Name, instanceId);

                    // Iniciar el workflow y esperar a que complete
                    await _repository.UpdateWorkflowExecutionStatusAsync(execution.Id, "En ejecución");
                    
                    var workflowId = await _workflowHost.StartWorkflow(definition.Name, version: definition.Version, data: dataObj, reference: instanceId);
                    
                    // Esperar a que el workflow termine
                    var status = await _workflowHost.GetStatus(workflowId);
                    while (status != WorkflowStatus.Complete && status != WorkflowStatus.Terminated)
                    {
                        await Task.Delay(500);
                        status = await _workflowHost.GetStatus(workflowId);
                    }

                    // Actualizar el estado de la ejecución
                    var finalStatus = status == WorkflowStatus.Complete ? "Completado" : "Terminado";
                    await _repository.CompleteWorkflowExecutionAsync(execution.Id, finalStatus, DateTime.UtcNow);

                    // Recargar la ejecución para tener los datos actualizados
                    execution = await _repository.GetWorkflowExecutionByIdAsync(execution.Id);

                    _logger.LogInformation("Completada ejecución de flujo de trabajo {Name} con instancia {InstanceId} y estado {Status}",
                        definition.Name, instanceId, finalStatus);

                    return execution;
                }
                catch (Exception ex)
                {
                    // En caso de error, actualizar el estado de la ejecución
                    await _repository.CompleteWorkflowExecutionAsync(execution.Id, "Error", DateTime.UtcNow, errorMessage: ex.Message);
                    
                    _logger.LogError(ex, "Error al ejecutar el flujo de trabajo {Name} con instancia {InstanceId}",
                        definition.Name, execution.InstanceId);
                    
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar el flujo de trabajo con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<string> StartWorkflowAsync(int workflowDefinitionId, string inputDataJson = null)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(workflowDefinitionId);
                if (definition == null)
                {
                    throw new ArgumentException($"No se encontró la definición de flujo de trabajo con ID {workflowDefinitionId}");
                }

                if (!definition.IsActive)
                {
                    throw new InvalidOperationException($"La definición de flujo de trabajo con ID {workflowDefinitionId} no está activa");
                }

                // Crear registro de ejecución en la base de datos
                var execution = new WorkflowExecution
                {
                    WorkflowDefinitionId = workflowDefinitionId,
                    WorkflowId = definition.Name,
                    InputDataJson = inputDataJson,
                    Status = "Pendiente"
                };

                execution = await _repository.CreateWorkflowExecutionAsync(execution);

                try
                {
                    // Preparar datos de entrada
                    var dataObj = PrepareWorkflowData(definition, inputDataJson, execution.Id);

                    // Generar un instanceId único
                    var instanceId = Guid.NewGuid().ToString();
                    execution.InstanceId = instanceId;
                    await _repository.UpdateWorkflowExecutionAsync(execution);

                    // Iniciar el workflow de forma asíncrona
                    await _repository.UpdateWorkflowExecutionStatusAsync(execution.Id, "Iniciando");
                    
                    var workflowId = await _workflowHost.StartWorkflow(definition.Name, version: definition.Version, data: dataObj, reference: instanceId);
                    
                    await _repository.UpdateWorkflowExecutionStatusAsync(execution.Id, "En ejecución");

                    _logger.LogInformation("Iniciado flujo de trabajo {Name} con instancia {InstanceId} de forma asíncrona",
                        definition.Name, instanceId);

                    return instanceId;
                }
                catch (Exception ex)
                {
                    // En caso de error, actualizar el estado de la ejecución
                    await _repository.CompleteWorkflowExecutionAsync(execution.Id, "Error", DateTime.UtcNow, errorMessage: ex.Message);
                    
                    _logger.LogError(ex, "Error al iniciar el flujo de trabajo {Name}",
                        definition.Name);
                    
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al iniciar el flujo de trabajo con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<bool> CancelWorkflowExecutionAsync(string instanceId)
        {
            try
            {
                var execution = await _repository.GetWorkflowExecutionByInstanceIdAsync(instanceId);
                if (execution == null)
                {
                    _logger.LogWarning("No se encontró la ejecución con InstanceId {InstanceId}", instanceId);
                    return false;
                }

                // Intentar terminar la ejecución en WorkflowCore
                await _workflowHost.TerminateWorkflow(instanceId);

                // Actualizar el estado en la base de datos
                await _repository.CompleteWorkflowExecutionAsync(execution.Id, "Cancelado", DateTime.UtcNow);

                _logger.LogInformation("Cancelada ejecución de flujo de trabajo con instancia {InstanceId}", instanceId);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cancelar la ejecución con instancia {InstanceId}", instanceId);
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

        public async Task<IEnumerable<WorkflowSchedule>> GetWorkflowSchedulesByDefinitionIdAsync(int workflowDefinitionId)
        {
            return await _repository.GetWorkflowSchedulesByDefinitionIdAsync(workflowDefinitionId);
        }

        public async Task<WorkflowSchedule> ScheduleWorkflowAsync(int workflowDefinitionId, string cronExpression, string timeZone, 
            string description = null, string inputDataJson = null, bool enabled = true, bool runImmediately = false)
        {
            try
            {
                var definition = await _repository.GetWorkflowDefinitionByIdAsync(workflowDefinitionId);
                if (definition == null)
                {
                    throw new ArgumentException($"No se encontró la definición de flujo de trabajo con ID {workflowDefinitionId}");
                }

                if (!definition.IsActive)
                {
                    throw new InvalidOperationException($"La definición de flujo de trabajo con ID {workflowDefinitionId} no está activa");
                }

                // Generar un jobId único
                var jobId = $"{definition.Name.Replace(" ", "-")}-{Guid.NewGuid().ToString().Substring(0, 8)}";

                // Crear la configuración de programación
                var config = new WorkflowScheduleConfig
                {
                    JobId = jobId,
                    WorkflowId = definition.Name,
                    WorkflowVersion = definition.Version,
                    CronExpression = cronExpression,
                    TimeZone = timeZone,
                    InputData = inputDataJson,
                    RunImmediately = runImmediately
                };

                // Programar el trabajo en Hangfire
                _scheduler.ScheduleWorkflow(config);

                // Crear el registro de programación en la base de datos
                var schedule = new WorkflowSchedule
                {
                    WorkflowDefinitionId = workflowDefinitionId,
                    JobId = jobId,
                    WorkflowId = definition.Name,
                    CronExpression = cronExpression,
                    TimeZone = timeZone,
                    Description = description,
                    InputDataJson = inputDataJson,
                    Enabled = enabled,
                    NextExecution = _scheduler.GetNextExecution(jobId)
                };

                return await _repository.CreateWorkflowScheduleAsync(schedule);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al programar el flujo de trabajo con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<WorkflowSchedule> UpdateWorkflowScheduleAsync(int id, string cronExpression, string timeZone, 
            string description = null, string inputDataJson = null, bool enabled = true)
        {
            try
            {
                var schedule = await _repository.GetWorkflowScheduleByIdAsync(id);
                if (schedule == null)
                {
                    throw new ArgumentException($"No se encontró la programación con ID {id}");
                }

                var definition = await _repository.GetWorkflowDefinitionByIdAsync(schedule.WorkflowDefinitionId);
                if (definition == null)
                {
                    throw new ArgumentException($"No se encontró la definición de flujo de trabajo asociada a la programación");
                }

                // Actualizar la programación en la base de datos
                schedule.CronExpression = cronExpression;
                schedule.TimeZone = timeZone;
                schedule.Description = description;
                schedule.InputDataJson = inputDataJson;
                schedule.Enabled = enabled;

                // Actualizar la programación en Hangfire
                var config = new WorkflowScheduleConfig
                {
                    JobId = schedule.JobId,
                    WorkflowId = definition.Name,
                    WorkflowVersion = definition.Version,
                    CronExpression = cronExpression,
                    TimeZone = timeZone,
                    InputData = inputDataJson
                };

                _scheduler.UpdateSchedule(config);

                // Actualizar la próxima ejecución
                schedule.NextExecution = _scheduler.GetNextExecution(schedule.JobId);

                return await _repository.UpdateWorkflowScheduleAsync(schedule);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la programación con ID {Id}", id);
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
                    _logger.LogWarning("No se encontró la programación con ID {Id}", id);
                    return false;
                }

                // Actualizar el estado en la base de datos
                var result = await _repository.SetWorkflowScheduleStatusAsync(id, enabled);

                // Si se deshabilitó, eliminar la programación en Hangfire
                if (!enabled)
                {
                    _scheduler.UnscheduleWorkflow(schedule.JobId);
                }
                else
                {
                    // Si se habilitó, volver a programar en Hangfire
                    var definition = await _repository.GetWorkflowDefinitionByIdAsync(schedule.WorkflowDefinitionId);
                    if (definition != null)
                    {
                        var config = new WorkflowScheduleConfig
                        {
                            JobId = schedule.JobId,
                            WorkflowId = definition.Name,
                            WorkflowVersion = definition.Version,
                            CronExpression = schedule.CronExpression,
                            TimeZone = schedule.TimeZone,
                            InputData = schedule.InputDataJson
                        };

                        _scheduler.ScheduleWorkflow(config);
                    }
                }

                return result;
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
                    _logger.LogWarning("No se encontró la programación con ID {Id}", id);
                    return false;
                }

                // Eliminar la programación en Hangfire
                _scheduler.UnscheduleWorkflow(schedule.JobId);

                // Eliminar el registro de la base de datos
                return await _repository.DeleteWorkflowScheduleAsync(id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al eliminar la programación con ID {Id}", id);
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
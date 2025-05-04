using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Persistence.Repositories
{
    /// <summary>
    /// Implementación del repositorio de flujos de trabajo
    /// </summary>
    public class WorkflowRepository : IWorkflowRepository
    {
        private readonly EtlOrchestratorDbContext _context;
        private readonly ILogger<WorkflowRepository> _logger;

        public WorkflowRepository(EtlOrchestratorDbContext context, ILogger<WorkflowRepository> logger)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        #region WorkflowDefinition

        public async Task<IEnumerable<WorkflowDefinition>> GetAllWorkflowDefinitionsAsync()
        {
            try
            {
                return await _context.WorkflowDefinitions
                    .AsNoTracking()
                    .OrderByDescending(w => w.LastModified)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener todas las definiciones de flujos de trabajo");
                throw;
            }
        }

        public async Task<WorkflowDefinition> GetWorkflowDefinitionByIdAsync(int id)
        {
            try
            {
                return await _context.WorkflowDefinitions
                    .AsNoTracking()
                    .FirstOrDefaultAsync(w => w.Id == id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la definición de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<WorkflowDefinition> GetWorkflowDefinitionByNameAndVersionAsync(string name, int version)
        {
            try
            {
                return await _context.WorkflowDefinitions
                    .AsNoTracking()
                    .FirstOrDefaultAsync(w => w.Name == name && w.Version == version);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la definición de flujo de trabajo con nombre {Name} y versión {Version}", name, version);
                throw;
            }
        }

        public async Task<WorkflowDefinition> GetLatestWorkflowDefinitionByNameAsync(string name)
        {
            try
            {
                return await _context.WorkflowDefinitions
                    .AsNoTracking()
                    .Where(w => w.Name == name && w.IsActive)
                    .OrderByDescending(w => w.Version)
                    .FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la última versión de la definición de flujo de trabajo con nombre {Name}", name);
                throw;
            }
        }

        public async Task<WorkflowDefinition> CreateWorkflowDefinitionAsync(WorkflowDefinition workflowDefinition)
        {
            try
            {
                // Verificar si ya existe una versión con el mismo nombre
                var existingVersion = await _context.WorkflowDefinitions
                    .Where(w => w.Name == workflowDefinition.Name)
                    .OrderByDescending(w => w.Version)
                    .FirstOrDefaultAsync();

                // Si existe, incrementar la versión
                if (existingVersion != null)
                {
                    workflowDefinition.Version = existingVersion.Version + 1;
                }
                else
                {
                    // Primera versión
                    workflowDefinition.Version = 1;
                }

                // Establecer fechas
                workflowDefinition.Created = DateTime.UtcNow;
                workflowDefinition.LastModified = DateTime.UtcNow;

                await _context.WorkflowDefinitions.AddAsync(workflowDefinition);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Creada definición de flujo de trabajo {Name} v{Version}", 
                    workflowDefinition.Name, workflowDefinition.Version);

                return workflowDefinition;
            }
            catch (Exception ex)
            {
                var sanitizedWorkflowName = workflowDefinition.Name?.Replace("\n", "").Replace("\r", "");
                _logger.LogError(ex, "Error al crear la definición de flujo de trabajo {Name}", sanitizedWorkflowName);
                throw;
            }
        }

        public async Task<WorkflowDefinition> UpdateWorkflowDefinitionAsync(WorkflowDefinition workflowDefinition)
        {
            try
            {
                // Actualizar fecha de modificación
                workflowDefinition.LastModified = DateTime.UtcNow;

                _context.WorkflowDefinitions.Update(workflowDefinition);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizada definición de flujo de trabajo {Name} v{Version}", 
                    workflowDefinition.Name, workflowDefinition.Version);

                return workflowDefinition;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la definición de flujo de trabajo con ID {Id}", workflowDefinition.Id);
                throw;
            }
        }

        public async Task<bool> SetWorkflowDefinitionStatusAsync(int id, bool isActive)
        {
            try
            {
                var definition = await _context.WorkflowDefinitions.FindAsync(id);
                if (definition == null)
                {
                    _logger.LogWarning("No se encontró la definición de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                definition.IsActive = isActive;
                definition.LastModified = DateTime.UtcNow;

                await _context.SaveChangesAsync();

                _logger.LogInformation("Definición de flujo de trabajo {Name} v{Version} {Status}", 
                    definition.Name, definition.Version, isActive ? "activada" : "desactivada");

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cambiar el estado de la definición de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        #endregion

        #region WorkflowExecution

        public async Task<IEnumerable<WorkflowExecution>> GetAllWorkflowExecutionsAsync()
        {
            try
            {
                return await _context.WorkflowExecutions
                    .AsNoTracking()
                    .OrderByDescending(e => e.StartTime)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener todas las ejecuciones de flujos de trabajo");
                throw;
            }
        }

        public async Task<IEnumerable<WorkflowExecution>> GetWorkflowExecutionsByDefinitionIdAsync(int workflowDefinitionId)
        {
            try
            {
                return await _context.WorkflowExecutions
                    .AsNoTracking()
                    .Where(e => e.WorkflowDefinitionId == workflowDefinitionId)
                    .OrderByDescending(e => e.StartTime)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener las ejecuciones para la definición de flujo de trabajo con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<WorkflowExecution> GetWorkflowExecutionByIdAsync(int id)
        {
            try
            {
                return await _context.WorkflowExecutions
                    .AsNoTracking()
                    .Include(e => e.Steps)
                    .FirstOrDefaultAsync(e => e.Id == id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la ejecución de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<WorkflowExecution> GetWorkflowExecutionByInstanceIdAsync(string instanceId)
        {
            try
            {
                return await _context.WorkflowExecutions
                    .AsNoTracking()
                    .Include(e => e.Steps)
                    .FirstOrDefaultAsync(e => e.InstanceId == instanceId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la ejecución de flujo de trabajo con InstanceId {InstanceId}", instanceId);
                throw;
            }
        }

        public async Task<WorkflowExecution> CreateWorkflowExecutionAsync(WorkflowExecution workflowExecution)
        {
            try
            {
                // Establecer hora de inicio
                workflowExecution.StartTime = DateTime.UtcNow;

                await _context.WorkflowExecutions.AddAsync(workflowExecution);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Creada ejecución de flujo de trabajo con ID {WorkflowId} e instancia {InstanceId}", 
                    workflowExecution.WorkflowId, workflowExecution.InstanceId);

                return workflowExecution;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear la ejecución de flujo de trabajo");
                throw;
            }
        }

        public async Task<WorkflowExecution> UpdateWorkflowExecutionAsync(WorkflowExecution workflowExecution)
        {
            try
            {
                _context.WorkflowExecutions.Update(workflowExecution);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizada ejecución de flujo de trabajo con ID {Id}", workflowExecution.Id);

                return workflowExecution;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la ejecución de flujo de trabajo con ID {Id}", workflowExecution.Id);
                throw;
            }
        }

        public async Task<bool> UpdateWorkflowExecutionStatusAsync(int id, string status, string errorMessage = null)
        {
            try
            {
                var execution = await _context.WorkflowExecutions.FindAsync(id);
                if (execution == null)
                {
                    _logger.LogWarning("No se encontró la ejecución de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                execution.Status = status;
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    execution.ErrorMessage = errorMessage;
                }

                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizado estado de ejecución {Id} a {Status}", id, status);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar el estado de la ejecución de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> CompleteWorkflowExecutionAsync(int id, string status, DateTime endTime, string outputDataJson = null, string errorMessage = null)
        {
            try
            {
                var execution = await _context.WorkflowExecutions.FindAsync(id);
                if (execution == null)
                {
                    _logger.LogWarning("No se encontró la ejecución de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                execution.Status = status;
                execution.EndTime = endTime;
                
                if (!string.IsNullOrEmpty(outputDataJson))
                {
                    execution.OutputDataJson = outputDataJson;
                }
                
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    execution.ErrorMessage = errorMessage;
                }

                await _context.SaveChangesAsync();

                _logger.LogInformation("Completada ejecución {Id} con estado {Status}", id, status);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al completar la ejecución de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        #endregion

        #region WorkflowExecutionStep

        public async Task<IEnumerable<WorkflowExecutionStep>> GetWorkflowExecutionStepsByExecutionIdAsync(int workflowExecutionId)
        {
            try
            {
                return await _context.WorkflowExecutionSteps
                    .AsNoTracking()
                    .Where(s => s.WorkflowExecutionId == workflowExecutionId)
                    .OrderBy(s => s.StepOrder)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener los pasos de ejecución para la ejecución con ID {Id}", workflowExecutionId);
                throw;
            }
        }

        public async Task<WorkflowExecutionStep> GetWorkflowExecutionStepByIdAsync(int id)
        {
            try
            {
                return await _context.WorkflowExecutionSteps
                    .AsNoTracking()
                    .FirstOrDefaultAsync(s => s.Id == id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener el paso de ejecución con ID {Id}", id);
                throw;
            }
        }

        public async Task<WorkflowExecutionStep> CreateWorkflowExecutionStepAsync(WorkflowExecutionStep workflowExecutionStep)
        {
            try
            {
                // Establecer hora de inicio
                workflowExecutionStep.StartTime = DateTime.UtcNow;

                await _context.WorkflowExecutionSteps.AddAsync(workflowExecutionStep);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Creado paso de ejecución {StepName} para la ejecución {ExecutionId}", 
                    workflowExecutionStep.StepName, workflowExecutionStep.WorkflowExecutionId);

                return workflowExecutionStep;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear el paso de ejecución {StepName}", workflowExecutionStep.StepName);
                throw;
            }
        }

        public async Task<WorkflowExecutionStep> UpdateWorkflowExecutionStepAsync(WorkflowExecutionStep workflowExecutionStep)
        {
            try
            {
                _context.WorkflowExecutionSteps.Update(workflowExecutionStep);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizado paso de ejecución con ID {Id}", workflowExecutionStep.Id);

                return workflowExecutionStep;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar el paso de ejecución con ID {Id}", workflowExecutionStep.Id);
                throw;
            }
        }

        public async Task<bool> UpdateWorkflowExecutionStepStatusAsync(int id, string status, string errorMessage = null)
        {
            try
            {
                var step = await _context.WorkflowExecutionSteps.FindAsync(id);
                if (step == null)
                {
                    _logger.LogWarning("No se encontró el paso de ejecución con ID {Id}", id);
                    return false;
                }

                step.Status = status;
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    step.ErrorMessage = errorMessage;
                }

                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizado estado del paso {Id} a {Status}", id, status);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar el estado del paso de ejecución con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> CompleteWorkflowExecutionStepAsync(int id, string status, DateTime endTime, string outputDataJson = null, 
            int? recordsProcessed = null, int? recordsWithErrors = null, string errorMessage = null)
        {
            try
            {
                var step = await _context.WorkflowExecutionSteps.FindAsync(id);
                if (step == null)
                {
                    _logger.LogWarning("No se encontró el paso de ejecución con ID {Id}", id);
                    return false;
                }

                step.Status = status;
                step.EndTime = endTime;
                
                if (!string.IsNullOrEmpty(outputDataJson))
                {
                    step.OutputDataJson = outputDataJson;
                }
                
                if (recordsProcessed.HasValue)
                {
                    step.RecordsProcessed = recordsProcessed;
                }
                
                if (recordsWithErrors.HasValue)
                {
                    step.RecordsWithErrors = recordsWithErrors;
                }
                
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    step.ErrorMessage = errorMessage;
                }

                await _context.SaveChangesAsync();

                _logger.LogInformation("Completado paso {Id} con estado {Status}", id, status);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al completar el paso de ejecución con ID {Id}", id);
                throw;
            }
        }

        #endregion

        #region WorkflowSchedule

        public async Task<IEnumerable<WorkflowSchedule>> GetAllWorkflowSchedulesAsync()
        {
            try
            {
                return await _context.WorkflowSchedules
                    .AsNoTracking()
                    .Include(s => s.WorkflowDefinition)
                    .OrderBy(s => s.WorkflowDefinition.Name)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener todas las programaciones de flujos de trabajo");
                throw;
            }
        }

        public async Task<IEnumerable<WorkflowSchedule>> GetWorkflowSchedulesByDefinitionIdAsync(int workflowDefinitionId)
        {
            try
            {
                return await _context.WorkflowSchedules
                    .AsNoTracking()
                    .Where(s => s.WorkflowDefinitionId == workflowDefinitionId)
                    .OrderBy(s => s.Created)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener las programaciones para la definición de flujo de trabajo con ID {Id}", workflowDefinitionId);
                throw;
            }
        }

        public async Task<WorkflowSchedule> GetWorkflowScheduleByIdAsync(int id)
        {
            try
            {
                return await _context.WorkflowSchedules
                    .AsNoTracking()
                    .Include(s => s.WorkflowDefinition)
                    .FirstOrDefaultAsync(s => s.Id == id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la programación de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<WorkflowSchedule> GetWorkflowScheduleByJobIdAsync(string jobId)
        {
            try
            {
                return await _context.WorkflowSchedules
                    .AsNoTracking()
                    .Include(s => s.WorkflowDefinition)
                    .FirstOrDefaultAsync(s => s.JobId == jobId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener la programación de flujo de trabajo con JobId {JobId}", jobId);
                throw;
            }
        }

        public async Task<WorkflowSchedule> CreateWorkflowScheduleAsync(WorkflowSchedule workflowSchedule)
        {
            try
            {
                // Establecer fechas
                workflowSchedule.Created = DateTime.UtcNow;
                workflowSchedule.LastModified = DateTime.UtcNow;

                await _context.WorkflowSchedules.AddAsync(workflowSchedule);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Creada programación de flujo de trabajo con JobId {JobId}", workflowSchedule.JobId);

                return workflowSchedule;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al crear la programación de flujo de trabajo");
                throw;
            }
        }

        public async Task<WorkflowSchedule> UpdateWorkflowScheduleAsync(WorkflowSchedule workflowSchedule)
        {
            try
            {
                // Actualizar fecha de modificación
                workflowSchedule.LastModified = DateTime.UtcNow;

                _context.WorkflowSchedules.Update(workflowSchedule);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizada programación de flujo de trabajo con ID {Id}", workflowSchedule.Id);

                return workflowSchedule;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la programación de flujo de trabajo con ID {Id}", workflowSchedule.Id);
                throw;
            }
        }

        public async Task<bool> SetWorkflowScheduleStatusAsync(int id, bool enabled)
        {
            try
            {
                var schedule = await _context.WorkflowSchedules.FindAsync(id);
                if (schedule == null)
                {
                    _logger.LogWarning("No se encontró la programación de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                schedule.Enabled = enabled;
                schedule.LastModified = DateTime.UtcNow;

                await _context.SaveChangesAsync();

                _logger.LogInformation("Programación de flujo de trabajo {Id} {Status}", id, enabled ? "habilitada" : "deshabilitada");

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cambiar el estado de la programación de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> UpdateWorkflowScheduleExecutionMetadataAsync(int id, DateTime? lastExecution, DateTime? nextExecution)
        {
            try
            {
                var schedule = await _context.WorkflowSchedules.FindAsync(id);
                if (schedule == null)
                {
                    _logger.LogWarning("No se encontró la programación de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                if (lastExecution.HasValue)
                {
                    schedule.LastExecution = lastExecution;
                }

                if (nextExecution.HasValue)
                {
                    schedule.NextExecution = nextExecution;
                }

                schedule.LastModified = DateTime.UtcNow;

                await _context.SaveChangesAsync();

                _logger.LogInformation("Actualizados metadatos de ejecución para la programación {Id}", id);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar los metadatos de ejecución de la programación con ID {Id}", id);
                throw;
            }
        }

        public async Task<bool> DeleteWorkflowScheduleAsync(int id)
        {
            try
            {
                var schedule = await _context.WorkflowSchedules.FindAsync(id);
                if (schedule == null)
                {
                    _logger.LogWarning("No se encontró la programación de flujo de trabajo con ID {Id}", id);
                    return false;
                }

                _context.WorkflowSchedules.Remove(schedule);
                await _context.SaveChangesAsync();

                _logger.LogInformation("Eliminada programación de flujo de trabajo con ID {Id}", id);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al eliminar la programación de flujo de trabajo con ID {Id}", id);
                throw;
            }
        }

        #endregion

        #region WorkflowLog

        public async Task<WorkflowLog> CreateWorkflowLogAsync(WorkflowLog workflowLog)
        {
            try
            {
                // Asegurar que la marca de tiempo esté establecida
                if (workflowLog.Timestamp == default)
                {
                    workflowLog.Timestamp = DateTime.UtcNow;
                }

                await _context.WorkflowLogs.AddAsync(workflowLog);
                
                // Usar método que no dispara eventos para evitar recursión
                await _context.Database.ExecuteSqlRawAsync(
                    "INSERT INTO WorkflowLogs (Timestamp, LogLevel, Category, Message, Exception, WorkflowId, InstanceId, StepName, AdditionalData) " +
                    "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})",
                    workflowLog.Timestamp, workflowLog.LogLevel, workflowLog.Category, workflowLog.Message, 
                    workflowLog.Exception ?? "", workflowLog.WorkflowId ?? "", workflowLog.InstanceId ?? "", 
                    workflowLog.StepName ?? "", workflowLog.AdditionalData ?? "");
                
                // No usar SaveChangesAsync porque provocaría recursión

                return workflowLog;
            }
            catch (Exception ex)
            {
                // No registramos el error para evitar recursión
                Console.WriteLine($"Error al crear registro de log: {ex.Message}");
                return workflowLog; // Devolvemos el objeto incluso si falló para evitar errores en cascada
            }
        }

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByWorkflowIdAsync(string workflowId)
        {
            try
            {
                return await _context.WorkflowLogs
                    .AsNoTracking()
                    .Where(l => l.WorkflowId == workflowId)
                    .OrderByDescending(l => l.Timestamp)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener logs para el flujo de trabajo con ID {WorkflowId}", workflowId);
                throw;
            }
        }

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByInstanceIdAsync(string instanceId)
        {
            try
            {
                return await _context.WorkflowLogs
                    .AsNoTracking()
                    .Where(l => l.InstanceId == instanceId)
                    .OrderByDescending(l => l.Timestamp)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener logs para la instancia con ID {InstanceId}", instanceId);
                throw;
            }
        }

        public async Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByDateRangeAsync(DateTime startDate, DateTime endDate)
        {
            try
            {
                return await _context.WorkflowLogs
                    .AsNoTracking()
                    .Where(l => l.Timestamp >= startDate && l.Timestamp <= endDate)
                    .OrderByDescending(l => l.Timestamp)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al obtener logs para el rango de fechas {StartDate} - {EndDate}", startDate, endDate);
                throw;
            }
        }

        #endregion
    }
} 
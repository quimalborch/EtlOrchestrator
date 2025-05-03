using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlOrchestrator.Infrastructure.Scheduler;

namespace EtlOrchestrator.Infrastructure.Services
{
    /// <summary>
    /// Interfaz para el servicio de gestión de flujos de trabajo ETL
    /// </summary>
    public interface IEtlWorkflowService
    {
        #region Workflow Definition Management

        /// <summary>
        /// Obtiene todas las definiciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowDefinition>> GetAllWorkflowDefinitionsAsync();

        /// <summary>
        /// Obtiene una definición de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowDefinition> GetWorkflowDefinitionByIdAsync(int id);

        /// <summary>
        /// Crea una nueva definición de flujo de trabajo
        /// </summary>
        Task<WorkflowDefinition> CreateWorkflowDefinitionAsync(string name, string description, string configurationJson);

        /// <summary>
        /// Actualiza una definición de flujo de trabajo existente
        /// </summary>
        Task<WorkflowDefinition> UpdateWorkflowDefinitionAsync(int id, string description, string configurationJson);

        /// <summary>
        /// Activa o desactiva una definición de flujo de trabajo
        /// </summary>
        Task<bool> SetWorkflowDefinitionStatusAsync(int id, bool isActive);

        #endregion

        #region Workflow Execution Management

        /// <summary>
        /// Obtiene todas las ejecuciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowExecution>> GetAllWorkflowExecutionsAsync();

        /// <summary>
        /// Obtiene una ejecución de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowExecution> GetWorkflowExecutionByIdAsync(int id);

        /// <summary>
        /// Obtiene las ejecuciones de un flujo de trabajo específico
        /// </summary>
        Task<IEnumerable<WorkflowExecution>> GetWorkflowExecutionsByDefinitionIdAsync(int workflowDefinitionId);

        /// <summary>
        /// Ejecuta un flujo de trabajo de forma sincrónica
        /// </summary>
        Task<WorkflowExecution> ExecuteWorkflowAsync(int workflowDefinitionId, string inputDataJson = null);

        /// <summary>
        /// Ejecuta un flujo de trabajo de forma asincrónica
        /// </summary>
        Task<string> StartWorkflowAsync(int workflowDefinitionId, string inputDataJson = null);

        /// <summary>
        /// Cancela una ejecución de flujo de trabajo en curso
        /// </summary>
        Task<bool> CancelWorkflowExecutionAsync(string instanceId);

        #endregion

        #region Workflow Schedule Management

        /// <summary>
        /// Obtiene todas las programaciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowSchedule>> GetAllWorkflowSchedulesAsync();

        /// <summary>
        /// Obtiene una programación de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowSchedule> GetWorkflowScheduleByIdAsync(int id);

        /// <summary>
        /// Obtiene las programaciones para un flujo de trabajo específico
        /// </summary>
        Task<IEnumerable<WorkflowSchedule>> GetWorkflowSchedulesByDefinitionIdAsync(int workflowDefinitionId);

        /// <summary>
        /// Crea una nueva programación para un flujo de trabajo
        /// </summary>
        Task<WorkflowSchedule> ScheduleWorkflowAsync(int workflowDefinitionId, string cronExpression, string timeZone, 
            string description = null, string inputDataJson = null, bool enabled = true, bool runImmediately = false);

        /// <summary>
        /// Actualiza una programación existente
        /// </summary>
        Task<WorkflowSchedule> UpdateWorkflowScheduleAsync(int id, string cronExpression, string timeZone, 
            string description = null, string inputDataJson = null, bool enabled = true);

        /// <summary>
        /// Activa o desactiva una programación
        /// </summary>
        Task<bool> SetWorkflowScheduleStatusAsync(int id, bool enabled);

        /// <summary>
        /// Elimina una programación
        /// </summary>
        Task<bool> DeleteWorkflowScheduleAsync(int id);

        #endregion

        #region Workflow Logs

        /// <summary>
        /// Obtiene logs por identificador de flujo de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByWorkflowIdAsync(string workflowId);

        /// <summary>
        /// Obtiene logs por identificador de instancia
        /// </summary>
        Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByInstanceIdAsync(string instanceId);

        /// <summary>
        /// Obtiene logs por rango de fechas
        /// </summary>
        Task<IEnumerable<WorkflowLog>> GetWorkflowLogsByDateRangeAsync(DateTime startDate, DateTime endDate);

        #endregion
    }
} 
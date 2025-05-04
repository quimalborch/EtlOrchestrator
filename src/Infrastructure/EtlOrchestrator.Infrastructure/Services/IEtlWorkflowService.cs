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
        /// Ejecuta un flujo de trabajo
        /// </summary>
        Task<WorkflowExecution> ExecuteWorkflowAsync(int workflowDefinitionId, string inputDataJson);

        /// <summary>
        /// Obtiene todos los pasos de una ejecución de flujo de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowExecutionStep>> GetWorkflowExecutionStepsAsync(int executionId);

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
        /// Crea una nueva programación para un flujo de trabajo
        /// </summary>
        Task<WorkflowSchedule> CreateWorkflowScheduleAsync(int workflowDefinitionId, string cronExpression, string description, string inputDataJson);

        /// <summary>
        /// Activa o desactiva una programación de flujo de trabajo
        /// </summary>
        Task<bool> SetWorkflowScheduleStatusAsync(int id, bool enabled);

        /// <summary>
        /// Elimina una programación de flujo de trabajo
        /// </summary>
        Task<bool> DeleteWorkflowScheduleAsync(int id);

        /// <summary>
        /// Actualiza la información de ejecución de una programación
        /// </summary>
        Task UpdateScheduleExecutionInfoAsync(int scheduleId, DateTime lastExecution);

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
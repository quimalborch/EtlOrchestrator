using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Infrastructure.Persistence.Entities;

namespace EtlOrchestrator.Infrastructure.Persistence.Repositories
{
    /// <summary>
    /// Interfaz para el repositorio de flujos de trabajo
    /// </summary>
    public interface IWorkflowRepository
    {
        #region WorkflowDefinition

        /// <summary>
        /// Obtiene todas las definiciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowDefinition>> GetAllWorkflowDefinitionsAsync();

        /// <summary>
        /// Obtiene una definición de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowDefinition> GetWorkflowDefinitionByIdAsync(int id);

        /// <summary>
        /// Obtiene una definición de flujo de trabajo por su nombre y versión
        /// </summary>
        Task<WorkflowDefinition> GetWorkflowDefinitionByNameAndVersionAsync(string name, int version);

        /// <summary>
        /// Obtiene la última versión de una definición de flujo de trabajo por su nombre
        /// </summary>
        Task<WorkflowDefinition> GetLatestWorkflowDefinitionByNameAsync(string name);

        /// <summary>
        /// Crea una nueva definición de flujo de trabajo
        /// </summary>
        Task<WorkflowDefinition> CreateWorkflowDefinitionAsync(WorkflowDefinition workflowDefinition);

        /// <summary>
        /// Actualiza una definición de flujo de trabajo existente
        /// </summary>
        Task<WorkflowDefinition> UpdateWorkflowDefinitionAsync(WorkflowDefinition workflowDefinition);

        /// <summary>
        /// Activa o desactiva una definición de flujo de trabajo
        /// </summary>
        Task<bool> SetWorkflowDefinitionStatusAsync(int id, bool isActive);

        #endregion

        #region WorkflowExecution

        /// <summary>
        /// Obtiene todas las ejecuciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowExecution>> GetAllWorkflowExecutionsAsync();

        /// <summary>
        /// Obtiene las ejecuciones de flujos de trabajo para una definición específica
        /// </summary>
        Task<IEnumerable<WorkflowExecution>> GetWorkflowExecutionsByDefinitionIdAsync(int workflowDefinitionId);

        /// <summary>
        /// Obtiene una ejecución de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowExecution> GetWorkflowExecutionByIdAsync(int id);

        /// <summary>
        /// Obtiene una ejecución de flujo de trabajo por su ID de instancia
        /// </summary>
        Task<WorkflowExecution> GetWorkflowExecutionByInstanceIdAsync(string instanceId);

        /// <summary>
        /// Crea una nueva ejecución de flujo de trabajo
        /// </summary>
        Task<WorkflowExecution> CreateWorkflowExecutionAsync(WorkflowExecution workflowExecution);

        /// <summary>
        /// Actualiza una ejecución de flujo de trabajo existente
        /// </summary>
        Task<WorkflowExecution> UpdateWorkflowExecutionAsync(WorkflowExecution workflowExecution);

        /// <summary>
        /// Actualiza el estado de una ejecución de flujo de trabajo
        /// </summary>
        Task<bool> UpdateWorkflowExecutionStatusAsync(int id, string status, string errorMessage = null);

        /// <summary>
        /// Completa una ejecución de flujo de trabajo
        /// </summary>
        Task<bool> CompleteWorkflowExecutionAsync(int id, string status, DateTime endTime, string outputDataJson = null, string errorMessage = null);

        #endregion

        #region WorkflowExecutionStep

        /// <summary>
        /// Obtiene todos los pasos de ejecución para una ejecución específica de flujo de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowExecutionStep>> GetWorkflowExecutionStepsByExecutionIdAsync(int workflowExecutionId);

        /// <summary>
        /// Obtiene un paso de ejecución por su ID
        /// </summary>
        Task<WorkflowExecutionStep> GetWorkflowExecutionStepByIdAsync(int id);

        /// <summary>
        /// Crea un nuevo paso de ejecución de flujo de trabajo
        /// </summary>
        Task<WorkflowExecutionStep> CreateWorkflowExecutionStepAsync(WorkflowExecutionStep workflowExecutionStep);

        /// <summary>
        /// Actualiza un paso de ejecución de flujo de trabajo existente
        /// </summary>
        Task<WorkflowExecutionStep> UpdateWorkflowExecutionStepAsync(WorkflowExecutionStep workflowExecutionStep);

        /// <summary>
        /// Actualiza el estado de un paso de ejecución de flujo de trabajo
        /// </summary>
        Task<bool> UpdateWorkflowExecutionStepStatusAsync(int id, string status, string errorMessage = null);

        /// <summary>
        /// Completa un paso de ejecución de flujo de trabajo
        /// </summary>
        Task<bool> CompleteWorkflowExecutionStepAsync(int id, string status, DateTime endTime, string outputDataJson = null, 
            int? recordsProcessed = null, int? recordsWithErrors = null, string errorMessage = null);

        #endregion

        #region WorkflowSchedule

        /// <summary>
        /// Obtiene todas las programaciones de flujos de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowSchedule>> GetAllWorkflowSchedulesAsync();

        /// <summary>
        /// Obtiene las programaciones para una definición específica de flujo de trabajo
        /// </summary>
        Task<IEnumerable<WorkflowSchedule>> GetWorkflowSchedulesByDefinitionIdAsync(int workflowDefinitionId);

        /// <summary>
        /// Obtiene una programación de flujo de trabajo por su ID
        /// </summary>
        Task<WorkflowSchedule> GetWorkflowScheduleByIdAsync(int id);

        /// <summary>
        /// Obtiene una programación de flujo de trabajo por su ID de trabajo
        /// </summary>
        Task<WorkflowSchedule> GetWorkflowScheduleByJobIdAsync(string jobId);

        /// <summary>
        /// Crea una nueva programación de flujo de trabajo
        /// </summary>
        Task<WorkflowSchedule> CreateWorkflowScheduleAsync(WorkflowSchedule workflowSchedule);

        /// <summary>
        /// Actualiza una programación de flujo de trabajo existente
        /// </summary>
        Task<WorkflowSchedule> UpdateWorkflowScheduleAsync(WorkflowSchedule workflowSchedule);

        /// <summary>
        /// Activa o desactiva una programación de flujo de trabajo
        /// </summary>
        Task<bool> SetWorkflowScheduleStatusAsync(int id, bool enabled);

        /// <summary>
        /// Actualiza los metadatos de ejecución de una programación
        /// </summary>
        Task<bool> UpdateWorkflowScheduleExecutionMetadataAsync(int id, DateTime? lastExecution, DateTime? nextExecution);

        /// <summary>
        /// Elimina una programación de flujo de trabajo
        /// </summary>
        Task<bool> DeleteWorkflowScheduleAsync(int id);

        #endregion

        #region WorkflowLog

        /// <summary>
        /// Crea un nuevo registro de log
        /// </summary>
        Task<WorkflowLog> CreateWorkflowLogAsync(WorkflowLog workflowLog);

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
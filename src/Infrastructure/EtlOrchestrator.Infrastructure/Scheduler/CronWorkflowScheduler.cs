using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cronos;
using EtlOrchestrator.Core;
using EtlOrchestrator.Infrastructure.Services;
using EtlWorkflowRepo = EtlOrchestrator.Infrastructure.Persistence.Repositories;
using Hangfire;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;

namespace EtlOrchestrator.Infrastructure.Scheduler
{
    /// <summary>
    /// Configuración para programar un flujo de trabajo
    /// </summary>
    public class WorkflowScheduleConfig
    {
        /// <summary>
        /// ID único del trabajo programado
        /// </summary>
        public string JobId { get; set; }
        
        /// <summary>
        /// ID del flujo de trabajo a ejecutar
        /// </summary>
        public string WorkflowId { get; set; }
        
        /// <summary>
        /// Versión del flujo de trabajo
        /// </summary>
        public int WorkflowVersion { get; set; } = 1;
        
        /// <summary>
        /// Expresión Cron que define cuándo ejecutar el trabajo
        /// </summary>
        public string CronExpression { get; set; }
        
        /// <summary>
        /// Zona horaria para la expresión Cron (UTC por defecto)
        /// </summary>
        public string TimeZone { get; set; } = "UTC";
        
        /// <summary>
        /// Datos de entrada para el flujo de trabajo
        /// </summary>
        public object InputData { get; set; }
        
        /// <summary>
        /// Indica si el trabajo se ejecutará inmediatamente al programarlo
        /// </summary>
        public bool RunImmediately { get; set; }
    }

    /// <summary>
    /// Programador de workflows basado en expresiones cron utilizando Hangfire
    /// </summary>
    public class CronWorkflowScheduler
    {
        private readonly ILogger<CronWorkflowScheduler> _logger;
        private readonly IRecurringJobManager _recurringJobManager;
        private readonly IBackgroundJobClient _backgroundJobClient;

        public CronWorkflowScheduler(
            ILogger<CronWorkflowScheduler> logger,
            IRecurringJobManager recurringJobManager,
            IBackgroundJobClient backgroundJobClient)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _recurringJobManager = recurringJobManager ?? throw new ArgumentNullException(nameof(recurringJobManager));
            _backgroundJobClient = backgroundJobClient ?? throw new ArgumentNullException(nameof(backgroundJobClient));
        }

        /// <summary>
        /// Programa la ejecución recurrente de un workflow
        /// </summary>
        /// <param name="workflowDefinitionId">ID de la definición del workflow</param>
        /// <param name="workflowName">Nombre del workflow</param>
        /// <param name="scheduleId">ID de la programación</param>
        /// <param name="cronExpression">Expresión cron</param>
        /// <param name="inputDataJson">Datos de entrada en formato JSON</param>
        /// <returns>ID del trabajo programado</returns>
        public string ScheduleWorkflow(
            int workflowDefinitionId,
            string workflowName,
            int scheduleId,
            string cronExpression,
            string inputDataJson)
        {
            try
            {
                // Generar un ID único para el trabajo
                string jobId = $"workflow-{workflowDefinitionId}-schedule-{scheduleId}";

                // Registrar el trabajo recurrente en Hangfire
                _recurringJobManager.AddOrUpdate<WorkflowExecutionJob>(
                    jobId,
                    job => job.ExecuteWorkflowAsync(workflowDefinitionId, scheduleId, workflowName, inputDataJson),
                    cronExpression);

                // Sanitize the cronExpression to prevent log forging
                string sanitizedCronExpression = cronExpression.Replace(Environment.NewLine, "").Replace("\n", "").Replace("\r", "");
                _logger.LogInformation("Workflow {WorkflowName} programado con cron {CronExpression}, jobId: {JobId}",
                    workflowName, sanitizedCronExpression, jobId);

                return jobId;
            }
            catch (Exception ex)
            {
                string sanitizedCronExpression = cronExpression.Replace(Environment.NewLine, "").Replace("\n", "").Replace("\r", "");
                _logger.LogError(ex, "Error al programar el workflow {WorkflowName} con cron {CronExpression}",
                    workflowName, sanitizedCronExpression);
                throw;
            }
        }

        /// <summary>
        /// Ejecuta un workflow inmediatamente
        /// </summary>
        /// <param name="workflowDefinitionId">ID de la definición del workflow</param>
        /// <param name="inputDataJson">Datos de entrada en formato JSON</param>
        /// <returns>ID del trabajo creado</returns>
        public string ExecuteWorkflowNow(int workflowDefinitionId, string inputDataJson)
        {
            try
            {
                // Encolar un trabajo para ejecución inmediata
                string jobId = _backgroundJobClient.Enqueue<WorkflowExecutionJob>(
                    job => job.ExecuteWorkflowAsync(workflowDefinitionId, null, null, inputDataJson));

                _logger.LogInformation("Workflow {WorkflowDefinitionId} encolado para ejecución inmediata, jobId: {JobId}",
                    workflowDefinitionId, jobId);

                return jobId;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al encolar el workflow {WorkflowDefinitionId} para ejecución inmediata",
                    workflowDefinitionId);
                throw;
            }
        }

        /// <summary>
        /// Elimina un trabajo programado
        /// </summary>
        /// <param name="jobId">ID del trabajo a eliminar</param>
        public void DeleteJob(string jobId)
        {
            try
            {
                _recurringJobManager.RemoveIfExists(jobId);
                _logger.LogInformation("Trabajo programado {JobId} eliminado", jobId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al eliminar el trabajo programado {JobId}", jobId);
                throw;
            }
        }

        /// <summary>
        /// Calcula el próximo tiempo de ejecución para una expresión cron
        /// </summary>
        /// <param name="cronExpression">Expresión cron</param>
        /// <returns>Fecha y hora de la próxima ejecución</returns>
        public DateTime? GetNextExecutionTime(string cronExpression)
        {
            try
            {
                var expression = CronExpression.Parse(cronExpression);
                return expression.GetNextOccurrence(DateTime.UtcNow, TimeZoneInfo.Utc);
            }
            catch (Exception ex)
            {
                var sanitizedCronExpression = cronExpression?.Replace("\n", "").Replace("\r", "");
                _logger.LogError(ex, "Error al calcular el próximo tiempo de ejecución para la expresión cron {CronExpression}",
                    sanitizedCronExpression);
                return null;
            }
        }
    }

    /// <summary>
    /// Clase de trabajo para la ejecución de workflows
    /// </summary>
    public class WorkflowExecutionJob
    {
        private readonly IEtlWorkflowService _workflowService;
        private readonly ILogger<WorkflowExecutionJob> _logger;

        public WorkflowExecutionJob(IEtlWorkflowService workflowService, ILogger<WorkflowExecutionJob> logger)
        {
            _workflowService = workflowService ?? throw new ArgumentNullException(nameof(workflowService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Ejecuta un workflow desde una tarea programada
        /// </summary>
        public async Task ExecuteWorkflowAsync(int workflowDefinitionId, int? scheduleId, string workflowName, string inputDataJson)
        {
            try
            {
                _logger.LogInformation("Iniciando ejecución programada del workflow {WorkflowName} (ID: {WorkflowDefinitionId})",
                    workflowName, workflowDefinitionId);

                // Ejecutar el workflow
                var execution = await _workflowService.ExecuteWorkflowAsync(workflowDefinitionId, inputDataJson);

                _logger.LogInformation("Workflow {WorkflowName} iniciado con éxito, ID de ejecución: {ExecutionId}",
                    workflowName, execution.Id);

                // Actualizar la información de la programación si es necesario
                if (scheduleId.HasValue)
                {
                    await UpdateScheduleExecutionInfo(scheduleId.Value);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar el workflow programado {WorkflowName} (ID: {WorkflowDefinitionId})",
                    workflowName, workflowDefinitionId);
            }
        }

        /// <summary>
        /// Actualiza la información de ejecución de la programación
        /// </summary>
        private async Task UpdateScheduleExecutionInfo(int scheduleId)
        {
            try
            {
                // Llamar al servicio para actualizar el estado de la programación
                await _workflowService.UpdateScheduleExecutionInfoAsync(scheduleId, DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al actualizar la información de ejecución para la programación {ScheduleId}",
                    scheduleId);
            }
        }
    }
} 
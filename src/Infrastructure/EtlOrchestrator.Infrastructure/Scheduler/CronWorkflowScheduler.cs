using System;
using System.Collections.Generic;
using Cronos;
using EtlOrchestrator.Core;
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
    /// Programador de flujos de trabajo basado en expresiones Cron utilizando Hangfire
    /// </summary>
    public class CronWorkflowScheduler
    {
        private readonly IWorkflowHost _workflowHost;
        private readonly IRecurringJobManager _recurringJobManager;
        private readonly ILogger<CronWorkflowScheduler> _logger;
        
        // Registro de trabajos programados
        private readonly Dictionary<string, WorkflowScheduleConfig> _scheduledJobs = new Dictionary<string, WorkflowScheduleConfig>();

        public CronWorkflowScheduler(
            IWorkflowHost workflowHost,
            IRecurringJobManager recurringJobManager,
            ILogger<CronWorkflowScheduler> logger)
        {
            _workflowHost = workflowHost ?? throw new ArgumentNullException(nameof(workflowHost));
            _recurringJobManager = recurringJobManager ?? throw new ArgumentNullException(nameof(recurringJobManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Programa un flujo de trabajo para ejecutarse según una expresión Cron
        /// </summary>
        /// <param name="config">Configuración del trabajo programado</param>
        public void ScheduleWorkflow(WorkflowScheduleConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
                
            if (string.IsNullOrEmpty(config.JobId))
                throw new ArgumentException("El ID del trabajo es obligatorio", nameof(config.JobId));
                
            if (string.IsNullOrEmpty(config.WorkflowId))
                throw new ArgumentException("El ID del flujo de trabajo es obligatorio", nameof(config.WorkflowId));
                
            if (string.IsNullOrEmpty(config.CronExpression))
                throw new ArgumentException("La expresión Cron es obligatoria", nameof(config.CronExpression));
                
            // Validar la expresión Cron
            try
            {
                CronExpression.Parse(config.CronExpression);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Expresión Cron inválida: {ex.Message}", nameof(config.CronExpression));
            }
            
            // Registrar el trabajo programado
            _scheduledJobs[config.JobId] = config;
            
            // Crear el trabajo recurrente en Hangfire
            _recurringJobManager.AddOrUpdate(
                config.JobId,
                () => ExecuteWorkflow(config.JobId),
                config.CronExpression,
                TimeZoneInfo.FindSystemTimeZoneById(config.TimeZone)
            );
            
            _logger.LogInformation("Trabajo programado: {JobId}, Workflow: {WorkflowId}, Cron: {CronExpression}, TimeZone: {TimeZone}",
                config.JobId, config.WorkflowId, config.CronExpression, config.TimeZone);
                
            // Ejecutar inmediatamente si es necesario
            if (config.RunImmediately)
            {
                _logger.LogInformation("Ejecutando trabajo inmediatamente: {JobId}", config.JobId);
                BackgroundJob.Enqueue(() => ExecuteWorkflow(config.JobId));
            }
        }

        /// <summary>
        /// Elimina un trabajo programado
        /// </summary>
        /// <param name="jobId">ID del trabajo a eliminar</param>
        public void UnscheduleWorkflow(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentException("El ID del trabajo es obligatorio", nameof(jobId));
                
            if (_scheduledJobs.ContainsKey(jobId))
            {
                _recurringJobManager.RemoveIfExists(jobId);
                _scheduledJobs.Remove(jobId);
                
                _logger.LogInformation("Trabajo desprogramado: {JobId}", jobId);
            }
            else
            {
                _logger.LogWarning("Intento de desprogramar un trabajo que no existe: {JobId}", jobId);
            }
        }

        /// <summary>
        /// Actualiza la configuración de un trabajo programado existente
        /// </summary>
        /// <param name="config">Nueva configuración del trabajo</param>
        public void UpdateSchedule(WorkflowScheduleConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
                
            if (string.IsNullOrEmpty(config.JobId))
                throw new ArgumentException("El ID del trabajo es obligatorio", nameof(config.JobId));
                
            // Eliminar el trabajo existente
            UnscheduleWorkflow(config.JobId);
            
            // Programar con la nueva configuración
            ScheduleWorkflow(config);
            
            _logger.LogInformation("Trabajo reprogramado: {JobId}", config.JobId);
        }

        /// <summary>
        /// Obtiene todos los trabajos programados
        /// </summary>
        public IReadOnlyDictionary<string, WorkflowScheduleConfig> GetScheduledJobs()
        {
            return _scheduledJobs;
        }

        /// <summary>
        /// Ejecuta un flujo de trabajo basado en su ID de trabajo
        /// </summary>
        /// <param name="jobId">ID del trabajo a ejecutar</param>
        public void ExecuteWorkflow(string jobId)
        {
            if (!_scheduledJobs.TryGetValue(jobId, out var config))
            {
                _logger.LogError("Intento de ejecutar un trabajo que no existe: {JobId}", jobId);
                return;
            }
            
            try
            {
                _logger.LogInformation("Iniciando ejecución de flujo de trabajo: {JobId}, Workflow: {WorkflowId}",
                    jobId, config.WorkflowId);
                    
                // Iniciar el flujo de trabajo
                string instanceId = _workflowHost.StartWorkflow(
                    config.WorkflowId,
                    config.WorkflowVersion,
                    config.InputData
                ).Result;
                
                _logger.LogInformation("Flujo de trabajo iniciado: {JobId}, Workflow: {WorkflowId}, Instancia: {InstanceId}",
                    jobId, config.WorkflowId, instanceId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al ejecutar flujo de trabajo: {JobId}, Workflow: {WorkflowId}, Error: {Message}",
                    jobId, config.WorkflowId, ex.Message);
            }
        }

        /// <summary>
        /// Calcula la próxima ejecución programada para un trabajo
        /// </summary>
        /// <param name="jobId">ID del trabajo</param>
        /// <returns>Fecha y hora de la próxima ejecución, o null si no se puede calcular</returns>
        public DateTime? GetNextExecution(string jobId)
        {
            if (!_scheduledJobs.TryGetValue(jobId, out var config))
            {
                _logger.LogWarning("Intento de obtener próxima ejecución de un trabajo que no existe: {JobId}", jobId);
                return null;
            }
            
            try
            {
                var cronExpression = CronExpression.Parse(config.CronExpression);
                var timeZone = TimeZoneInfo.FindSystemTimeZoneById(config.TimeZone);
                var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, timeZone);
                
                // Calcular la próxima ocurrencia
                var nextOccurrence = cronExpression.GetNextOccurrence(now, timeZone);
                
                return nextOccurrence;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al calcular próxima ejecución: {JobId}, Error: {Message}", jobId, ex.Message);
                return null;
            }
        }
    }
} 
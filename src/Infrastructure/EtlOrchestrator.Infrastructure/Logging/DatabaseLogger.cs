using System;
using EtlOrchestrator.Infrastructure.Persistence.Entities;
using EtlOrchestrator.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EtlOrchestrator.Infrastructure.Logging
{
    /// <summary>
    /// Logger personalizado para registrar eventos en la base de datos
    /// </summary>
    public class DatabaseLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly Func<IWorkflowRepository> _repositoryFactory;

        public DatabaseLogger(string categoryName, Func<IWorkflowRepository> repositoryFactory)
        {
            _categoryName = categoryName ?? throw new ArgumentNullException(nameof(categoryName));
            _repositoryFactory = repositoryFactory ?? throw new ArgumentNullException(nameof(repositoryFactory));
        }

        public IDisposable BeginScope<TState>(TState state) => new NoopDisposable();

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            if (formatter == null)
            {
                throw new ArgumentNullException(nameof(formatter));
            }

            var message = formatter(state, exception);

            if (string.IsNullOrEmpty(message) && exception == null)
            {
                return;
            }

            try
            {
                // Extraer metadatos específicos de workflow del scope (si están presentes)
                string workflowId = null;
                string instanceId = null;
                string stepName = null;
                object additionalData = null;

                if (state is IReadOnlyList<KeyValuePair<string, object>> logStateProperties)
                {
                    foreach (var prop in logStateProperties)
                    {
                        if (prop.Key == "WorkflowId") workflowId = prop.Value?.ToString();
                        else if (prop.Key == "InstanceId") instanceId = prop.Value?.ToString();
                        else if (prop.Key == "StepName") stepName = prop.Value?.ToString();
                        else if (prop.Key == "AdditionalData") additionalData = prop.Value;
                    }
                }

                var log = new WorkflowLog
                {
                    Timestamp = DateTime.UtcNow,
                    LogLevel = logLevel.ToString(),
                    Category = _categoryName,
                    Message = message,
                    Exception = exception?.ToString(),
                    WorkflowId = workflowId,
                    InstanceId = instanceId,
                    StepName = stepName,
                    AdditionalDataJson = additionalData != null ? JsonConvert.SerializeObject(additionalData) : null
                };

                // Obtener el repositorio y guardar el log
                var repository = _repositoryFactory();
                repository.CreateWorkflowLogAsync(log).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                // Evitar bucles infinitos de registro de errores
                Console.WriteLine($"Error al registrar en la base de datos: {ex.Message}");
            }
        }

        private class NoopDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
} 
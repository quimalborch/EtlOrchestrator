using System;
using EtlOrchestrator.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Logging
{
    /// <summary>
    /// Proveedor de logger para la base de datos
    /// </summary>
    [ProviderAlias("Database")]
    public class DatabaseLoggerProvider : ILoggerProvider
    {
        private readonly IServiceProvider _serviceProvider;

        public DatabaseLoggerProvider(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        }

        public ILogger CreateLogger(string categoryName)
        {
            return new DatabaseLogger(categoryName, () => _serviceProvider.CreateScope().ServiceProvider.GetRequiredService<IWorkflowRepository>());
        }

        public void Dispose()
        {
            // No hay recursos que limpiar
        }
    }

    /// <summary>
    /// Extensiones para configurar el logger de base de datos
    /// </summary>
    public static class DatabaseLoggerExtensions
    {
        /// <summary>
        /// Agrega el logger de base de datos al builder de logger
        /// </summary>
        public static ILoggingBuilder AddDatabase(this ILoggingBuilder builder)
        {
            builder.Services.AddSingleton<ILoggerProvider, DatabaseLoggerProvider>();
            return builder;
        }

        /// <summary>
        /// Agrega un scope de log con metadatos de workflow
        /// </summary>
        public static IDisposable BeginWorkflowScope(this ILogger logger, string workflowId = null, string instanceId = null, string stepName = null, object additionalData = null)
        {
            return logger.BeginScope(new Dictionary<string, object>
            {
                ["WorkflowId"] = workflowId,
                ["InstanceId"] = instanceId,
                ["StepName"] = stepName,
                ["AdditionalData"] = additionalData
            });
        }
    }
} 
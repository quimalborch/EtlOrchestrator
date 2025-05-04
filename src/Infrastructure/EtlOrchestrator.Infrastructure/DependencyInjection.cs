using System;
using EtlOrchestrator.Core.Connectors;
using EtlOrchestrator.Infrastructure.Connectors;
using EtlOrchestrator.Infrastructure.Logging;
using EtlOrchestrator.Infrastructure.Persistence;
using EtlOrchestrator.Infrastructure.Persistence.Repositories;
using EtlOrchestrator.Infrastructure.Scheduler;
using EtlOrchestrator.Infrastructure.Services;
using EtlOrchestrator.Infrastructure.Workflow;
using Hangfire;
using Hangfire.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace EtlOrchestrator.Infrastructure
{
    /// <summary>
    /// Clase de extensión para configurar la inyección de dependencias de la capa de infraestructura
    /// </summary>
    public static class DependencyInjection
    {
        /// <summary>
        /// Agrega los servicios de infraestructura al contenedor de servicios
        /// </summary>
        public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
        {
            // Registrar contexto de base de datos
            services.AddDbContext<EtlOrchestratorDbContext>(options =>
                options.UseSqlServer(
                    configuration.GetConnectionString("DefaultConnection"),
                    b => b.MigrationsAssembly(typeof(EtlOrchestratorDbContext).Assembly.FullName)));

            // Registrar conectores de origen
            services.AddScoped<ISourceConnector, SqlServerSourceConnector>();
            services.AddScoped<ISourceConnector, HttpApiSourceConnector>();
            services.AddScoped<ISourceConnector, CsvFileSourceConnector>();

            // Registrar transformaciones
            services.AddScoped<ITransform, DataCleanerTransform>();

            // Registrar conectores de carga
            services.AddScoped<ILoadConnector, SqlServerLoadConnector>();

            // Registrar repositorios
            services.AddScoped<EtlOrchestrator.Infrastructure.Persistence.Repositories.IWorkflowRepository, WorkflowRepository>();

            // Registrar servicios
            services.AddScoped<IEtlWorkflowService, EtlWorkflowService>();

            // Configurar WorkflowCore
            services.AddWorkflow();

            // Registrar workflows
            RegisterWorkflows(services);

            // Configurar Hangfire
            ConfigureHangfire(services, configuration);

            // Registrar el programador de workflows
            services.AddSingleton<CronWorkflowScheduler>();

            // Configurar logging
            ConfigureLogging(services);

            return services;
        }

        /// <summary>
        /// Registra los flujos de trabajo disponibles
        /// </summary>
        private static void RegisterWorkflows(IServiceCollection services)
        {
            // Registrar el flujo de trabajo simple
            services.AddTransient<IWorkflow<EtlWorkflowData>, SimpleEtlWorkflow>();

            // Aquí se pueden agregar más workflows a medida que se implementen
        }

        /// <summary>
        /// Configura Hangfire para la ejecución programada de trabajos
        /// </summary>
        private static void ConfigureHangfire(IServiceCollection services, IConfiguration configuration)
        {
            // Obtener la cadena de conexión para Hangfire
            string connectionString = configuration.GetConnectionString("HangfireConnection") ?? 
                                     configuration.GetConnectionString("DefaultConnection") ??
                                     throw new InvalidOperationException("No se encontró una cadena de conexión válida para Hangfire. Verifique que 'HangfireConnection' o 'DefaultConnection' estén configuradas en appsettings.json");

            // Configurar Hangfire con SQL Server
            services.AddHangfire(config => config
                .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UseSqlServerStorage(connectionString, 
                    new SqlServerStorageOptions
                    {
                        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
                        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
                        QueuePollInterval = TimeSpan.FromSeconds(15),
                        UseRecommendedIsolationLevel = true,
                        DisableGlobalLocks = true
                    }));

            // Agregar el servidor de procesamiento de Hangfire
            services.AddHangfireServer();
        }

        /// <summary>
        /// Configura el logging para la aplicación
        /// </summary>
        private static void ConfigureLogging(IServiceCollection services)
        {
            services.AddLogging(builder =>
            {
                // Añadir logger de consola para depuración
                builder.AddConsole();
                
                // Configurar filtros para evitar recursión
                builder.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);
                
                // Añadir el logger de base de datos después de filtrar los mensajes de EF
                builder.AddDatabase();
            });
        }
    }
} 
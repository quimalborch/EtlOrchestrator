using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using Newtonsoft.Json;

namespace EtlOrchestrator.Infrastructure.Workflow
{
    /// <summary>
    /// Implementación de un flujo de trabajo ETL simple: Extract -> Transform -> Load
    /// </summary>
    public class SimpleEtlWorkflow : IWorkflow<EtlWorkflowData>
    {
        private readonly ILogger<SimpleEtlWorkflow> _logger;
        private readonly ISourceConnector _sourceConnector;
        private readonly ITransform _transform;
        private readonly ILoadConnector _loadConnector;

        public SimpleEtlWorkflow(
            ISourceConnector sourceConnector,
            ITransform transform,
            ILoadConnector loadConnector,
            ILogger<SimpleEtlWorkflow> logger)
        {
            _sourceConnector = sourceConnector ?? throw new ArgumentNullException(nameof(sourceConnector));
            _transform = transform ?? throw new ArgumentNullException(nameof(transform));
            _loadConnector = loadConnector ?? throw new ArgumentNullException(nameof(loadConnector));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public string Id => "SimpleEtlWorkflow";
        public int Version => 1;

        public void Build(IWorkflowBuilder<EtlWorkflowData> builder)
        {
            builder
                .StartWith(context =>
                {
                    _logger.LogInformation("Iniciando flujo de trabajo ETL: {WorkflowId}", Id);
                    var data = context.Workflow.Data as EtlWorkflowData;
                    
                    // Si no se inicializó Context, hacerlo aquí
                    if (data.Context == null)
                    {
                        data.Context = new Context
                        {
                            JobName = Id,
                            ExecutionId = data.ExecutionId.ToString(),
                            StartTime = DateTime.UtcNow
                        };
                    }
                    
                    // Deserializar la configuración si es necesario
                    if (!string.IsNullOrEmpty(data.Configuration))
                    {
                        try
                        {
                            var config = JsonConvert.DeserializeObject<Dictionary<string, object>>(data.Configuration);
                            foreach (var key in config.Keys)
                            {
                                data.Context.SetParameter(key, config[key]);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error al deserializar la configuración del workflow");
                        }
                    }
                    
                    // Deserializar los datos de entrada si es necesario
                    if (!string.IsNullOrEmpty(data.InputData))
                    {
                        try
                        {
                            var inputData = JsonConvert.DeserializeObject<Dictionary<string, object>>(data.InputData);
                            foreach (var key in inputData.Keys)
                            {
                                data.Context.SetParameter("input_" + key, inputData[key]);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error al deserializar los datos de entrada del workflow");
                        }
                    }
                    
                    data.StartTime = DateTime.UtcNow;
                    data.Success = true;
                })
                .Then<ExtractStep>()
                    .OnError(WorkflowErrorHandling.Terminate)
                .Then<TransformStep>()
                    .OnError(WorkflowErrorHandling.Terminate)
                .Then<LoadStep>()
                    .OnError(WorkflowErrorHandling.Terminate)
                .Then(context =>
                {
                    var data = context.Workflow.Data as EtlWorkflowData;
                    data.EndTime = DateTime.UtcNow;
                    var duration = data.EndTime.Value - data.StartTime;
                    _logger.LogInformation("Flujo de trabajo ETL completado: {WorkflowId}. Duración: {Duration}", Id, duration);
                });
        }

        /// <summary>
        /// Paso de extracción de datos
        /// </summary>
        public class ExtractStep : StepBody
        {
            private readonly ISourceConnector _sourceConnector;
            private readonly ILogger<ExtractStep> _logger;

            public ExtractStep(ISourceConnector sourceConnector, ILogger<ExtractStep> logger)
            {
                _sourceConnector = sourceConnector;
                _logger = logger;
            }

            public override ExecutionResult Run(IStepExecutionContext context)
            {
                var data = context.Workflow.Data as EtlWorkflowData;
                
                _logger.LogInformation("Ejecutando paso de extracción");
                
                try
                {
                    data.ExtractedRecords = _sourceConnector.ExtractAsync(data.Context).GetAwaiter().GetResult();
                    
                    var recordCount = 0;
                    if (data.ExtractedRecords != null)
                    {
                        recordCount = data.ExtractedRecords is ICollection<Record> collection 
                            ? collection.Count 
                            : -1; // No podemos contar sin enumerar
                    }
                    
                    _logger.LogInformation("Extracción completada. Registros extraídos: {Count}", recordCount);
                    
                    return ExecutionResult.Next();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error en el paso de extracción: {Message}", ex.Message);
                    throw;
                }
            }
        }

        /// <summary>
        /// Paso de transformación de datos
        /// </summary>
        public class TransformStep : StepBody
        {
            private readonly ITransform _transform;
            private readonly ILogger<TransformStep> _logger;

            public TransformStep(ITransform transform, ILogger<TransformStep> logger)
            {
                _transform = transform;
                _logger = logger;
            }

            public override ExecutionResult Run(IStepExecutionContext context)
            {
                var data = context.Workflow.Data as EtlWorkflowData;
                
                _logger.LogInformation("Ejecutando paso de transformación");
                
                try
                {
                    if (data.ExtractedRecords == null)
                    {
                        _logger.LogWarning("No hay registros para transformar");
                        data.TransformedRecords = new Record[0];
                        return ExecutionResult.Next();
                    }
                    
                    data.TransformedRecords = _transform.TransformAsync(data.ExtractedRecords).GetAwaiter().GetResult();
                    
                    var recordCount = 0;
                    if (data.TransformedRecords != null)
                    {
                        recordCount = data.TransformedRecords is ICollection<Record> collection 
                            ? collection.Count 
                            : -1; // No podemos contar sin enumerar
                    }
                    
                    _logger.LogInformation("Transformación completada. Registros transformados: {Count}", recordCount);
                    
                    return ExecutionResult.Next();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error en el paso de transformación: {Message}", ex.Message);
                    throw;
                }
            }
        }

        /// <summary>
        /// Paso de carga de datos
        /// </summary>
        public class LoadStep : StepBody
        {
            private readonly ILoadConnector _loadConnector;
            private readonly ILogger<LoadStep> _logger;

            public LoadStep(ILoadConnector loadConnector, ILogger<LoadStep> logger)
            {
                _loadConnector = loadConnector;
                _logger = logger;
            }

            public override ExecutionResult Run(IStepExecutionContext context)
            {
                var data = context.Workflow.Data as EtlWorkflowData;
                
                _logger.LogInformation("Ejecutando paso de carga");
                
                try
                {
                    if (data.TransformedRecords == null)
                    {
                        _logger.LogWarning("No hay registros para cargar");
                        return ExecutionResult.Next();
                    }
                    
                    _loadConnector.LoadAsync(data.TransformedRecords).GetAwaiter().GetResult();
                    
                    _logger.LogInformation("Carga completada");
                    
                    return ExecutionResult.Next();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error en el paso de carga: {Message}", ex.Message);
                    throw;
                }
            }
        }
    }
    
    /// <summary>
    /// Manejador de errores global para el flujo de trabajo ETL
    /// </summary>
    public class EtlWorkflowErrorHandler : IWorkflowErrorHandler
    {
        private readonly ILogger<EtlWorkflowErrorHandler> _logger;
        
        public EtlWorkflowErrorHandler(ILogger<EtlWorkflowErrorHandler> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        
        public WorkflowErrorHandling Type => WorkflowErrorHandling.Terminate;
        
        public void Handle(WorkflowInstance workflow, WorkflowDefinition def, ExecutionPointer pointer, WorkflowStep step, Exception exception, Queue<ExecutionPointer> bubbleUpQueue)
        {
            var data = workflow.Data as EtlWorkflowData;
            if (data != null)
            {
                data.Success = false;
                data.ErrorMessage = exception.Message;
                data.EndTime = DateTime.UtcNow;
                
                var stepName = step?.Name ?? "Desconocido";
                _logger.LogError(exception, "Error en el paso '{StepName}': {Message}", stepName, exception.Message);
            }
        }
    }
} 
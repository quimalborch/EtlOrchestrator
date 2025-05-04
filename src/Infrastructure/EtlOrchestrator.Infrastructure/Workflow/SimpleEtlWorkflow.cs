using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace EtlOrchestrator.Infrastructure.Workflow
{
    /// <summary>
    /// Datos de contexto para el flujo de trabajo ETL
    /// </summary>
    public class EtlWorkflowData
    {
        public Context Context { get; set; }
        public IEnumerable<Record> ExtractedRecords { get; set; }
        public IEnumerable<Record> TransformedRecords { get; set; }
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
        public DateTime StartTime { get; set; } = DateTime.UtcNow;
        public DateTime? EndTime { get; set; }
    }

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
                    context.WorkflowData.StartTime = DateTime.UtcNow;
                    context.WorkflowData.Success = true;
                })
                .Then<ExtractStep>()
                    .OnError(WorkflowErrorHandling.Terminate, (data, context) =>
                    {
                        data.Success = false;
                        data.ErrorMessage = context.Exception.Message;
                        data.EndTime = DateTime.UtcNow;
                        _logger.LogError(context.Exception, "Error en el paso de extracción: {Message}", context.Exception.Message);
                    })
                .Then<TransformStep>()
                    .OnError(WorkflowErrorHandling.Terminate, (data, context) =>
                    {
                        data.Success = false;
                        data.ErrorMessage = context.Exception.Message;
                        data.EndTime = DateTime.UtcNow;
                        _logger.LogError(context.Exception, "Error en el paso de transformación: {Message}", context.Exception.Message);
                    })
                .Then<LoadStep>()
                    .OnError(WorkflowErrorHandling.Terminate, (data, context) =>
                    {
                        data.Success = false;
                        data.ErrorMessage = context.Exception.Message;
                        data.EndTime = DateTime.UtcNow;
                        _logger.LogError(context.Exception, "Error en el paso de carga: {Message}", context.Exception.Message);
                    })
                .Then(context =>
                {
                    context.WorkflowData.EndTime = DateTime.UtcNow;
                    var duration = context.WorkflowData.EndTime.Value - context.WorkflowData.StartTime;
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
} 
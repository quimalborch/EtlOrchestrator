using System;
using System.Collections.Generic;
using EtlOrchestrator.Core;

namespace EtlOrchestrator.Infrastructure.Workflow
{
    /// <summary>
    /// Datos de contexto para el flujo de trabajo ETL
    /// </summary>
    public class EtlWorkflowData
    {
        /// <summary>
        /// ID de la ejecución en la base de datos
        /// </summary>
        public int ExecutionId { get; set; }

        /// <summary>
        /// Configuración del workflow en formato JSON
        /// </summary>
        public string Configuration { get; set; }

        /// <summary>
        /// Datos de entrada en formato JSON
        /// </summary>
        public string InputData { get; set; }

        /// <summary>
        /// Contexto de ejecución
        /// </summary>
        public Context Context { get; set; }

        /// <summary>
        /// Registros extraídos en la fase de extracción
        /// </summary>
        public IEnumerable<Record> ExtractedRecords { get; set; }

        /// <summary>
        /// Registros transformados en la fase de transformación
        /// </summary>
        public IEnumerable<Record> TransformedRecords { get; set; }

        /// <summary>
        /// Indica si el workflow se ejecutó correctamente
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Mensaje de error en caso de fallo
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Fecha y hora de inicio de la ejecución
        /// </summary>
        public DateTime StartTime { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// Fecha y hora de finalización de la ejecución
        /// </summary>
        public DateTime? EndTime { get; set; }
    }
} 
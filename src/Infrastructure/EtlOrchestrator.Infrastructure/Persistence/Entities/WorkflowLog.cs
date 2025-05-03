using System;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa un registro de log de ejecución de flujos de trabajo
    /// </summary>
    public class WorkflowLog
    {
        /// <summary>
        /// Identificador único del registro de log
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Fecha y hora del registro
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Nivel de log (Information, Warning, Error, etc.)
        /// </summary>
        public string LogLevel { get; set; }

        /// <summary>
        /// Categoría del log
        /// </summary>
        public string Category { get; set; }

        /// <summary>
        /// Mensaje del log
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Excepción serializada (si existe)
        /// </summary>
        public string Exception { get; set; }

        /// <summary>
        /// Identificador del flujo de trabajo (si aplica)
        /// </summary>
        public string WorkflowId { get; set; }

        /// <summary>
        /// Identificador de la instancia de ejecución (si aplica)
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Nombre del paso donde se generó el log (si aplica)
        /// </summary>
        public string StepName { get; set; }

        /// <summary>
        /// Datos adicionales serializados como JSON
        /// </summary>
        public string AdditionalDataJson { get; set; }
    }
} 
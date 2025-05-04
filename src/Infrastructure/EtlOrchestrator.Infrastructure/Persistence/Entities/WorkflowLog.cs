using System;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa un registro de log del flujo de trabajo
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
        /// Nivel de log (Info, Warning, Error, etc.)
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
        /// Identificador del flujo de trabajo asociado
        /// </summary>
        public string WorkflowId { get; set; }

        /// <summary>
        /// Identificador de la instancia del flujo de trabajo
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Nombre del paso actual del flujo de trabajo
        /// </summary>
        public string StepName { get; set; }

        /// <summary>
        /// Datos adicionales serializados como JSON
        /// </summary>
        public string AdditionalData { get; set; }
    }
} 
using System;
using System.Collections.Generic;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa una ejecución de un flujo de trabajo ETL
    /// </summary>
    public class WorkflowExecution
    {
        /// <summary>
        /// Identificador único de la ejecución
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Identificador de la definición del flujo de trabajo
        /// </summary>
        public int WorkflowDefinitionId { get; set; }

        /// <summary>
        /// Referencia a la definición del flujo de trabajo
        /// </summary>
        public virtual WorkflowDefinition WorkflowDefinition { get; set; }

        /// <summary>
        /// Identificador del workflow (corresponde al ID de WorkflowCore)
        /// </summary>
        public string WorkflowId { get; set; }

        /// <summary>
        /// Identificador de la instancia (corresponde al InstanceId de WorkflowCore)
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Estado actual de la ejecución (En espera, En ejecución, Completado, Error, etc.)
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// Fecha y hora de inicio de la ejecución
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Fecha y hora de finalización de la ejecución
        /// </summary>
        public DateTime? EndTime { get; set; }

        /// <summary>
        /// Datos de entrada serializados como JSON
        /// </summary>
        public string InputDataJson { get; set; }

        /// <summary>
        /// Datos de salida serializados como JSON
        /// </summary>
        public string OutputDataJson { get; set; }

        /// <summary>
        /// Mensaje de error en caso de fallo
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Identificador del trabajo programado que inició esta ejecución (si aplica)
        /// </summary>
        public string ScheduledJobId { get; set; }

        /// <summary>
        /// Pasos de ejecución relacionados con esta ejecución
        /// </summary>
        public virtual ICollection<WorkflowExecutionStep> Steps { get; set; } = new List<WorkflowExecutionStep>();
    }
} 
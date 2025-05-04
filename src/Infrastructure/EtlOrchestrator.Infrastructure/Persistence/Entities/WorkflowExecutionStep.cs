using System;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa un paso en la ejecución de un flujo de trabajo ETL
    /// </summary>
    public class WorkflowExecutionStep
    {
        /// <summary>
        /// Identificador único del paso de ejecución
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Identificador de la ejecución a la que pertenece este paso
        /// </summary>
        public int WorkflowExecutionId { get; set; }

        /// <summary>
        /// Referencia a la ejecución del flujo de trabajo
        /// </summary>
        public virtual WorkflowExecution WorkflowExecution { get; set; }

        /// <summary>
        /// Nombre del paso (ExtractStep, TransformStep, LoadStep, etc.)
        /// </summary>
        public string StepName { get; set; }

        /// <summary>
        /// Tipo de paso (Extract, Transform, Load)
        /// </summary>
        public string StepType { get; set; }

        /// <summary>
        /// Orden de ejecución del paso
        /// </summary>
        public int StepOrder { get; set; }

        /// <summary>
        /// Estado actual del paso (En espera, En ejecución, Completado, Error, etc.)
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// Fecha y hora de inicio del paso
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Fecha y hora de finalización del paso
        /// </summary>
        public DateTime? EndTime { get; set; }

        /// <summary>
        /// Datos de entrada para el paso serializados como JSON
        /// </summary>
        public string InputDataJson { get; set; }

        /// <summary>
        /// Datos de salida del paso serializados como JSON
        /// </summary>
        public string OutputDataJson { get; set; }

        /// <summary>
        /// Número de registros procesados en este paso (si aplica)
        /// </summary>
        public int? RecordsProcessed { get; set; }

        /// <summary>
        /// Número de registros con errores en este paso (si aplica)
        /// </summary>
        public int? RecordsWithErrors { get; set; }

        /// <summary>
        /// Mensaje de error en caso de fallo
        /// </summary>
        public string ErrorMessage { get; set; }
    }
} 
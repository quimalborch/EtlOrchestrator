using System;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa una programación de ejecución periódica de un flujo de trabajo ETL
    /// </summary>
    public class WorkflowSchedule
    {
        /// <summary>
        /// Identificador único de la programación
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
        /// Identificador del trabajo en Hangfire
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// Identificador del workflow a ejecutar
        /// </summary>
        public string WorkflowId { get; set; }

        /// <summary>
        /// Expresión cron que define la programación
        /// </summary>
        public string CronExpression { get; set; }

        /// <summary>
        /// Zona horaria para la programación (por defecto UTC)
        /// </summary>
        public string TimeZone { get; set; } = "UTC";

        /// <summary>
        /// Descripción de la programación
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Datos de entrada serializados como JSON
        /// </summary>
        public string InputDataJson { get; set; }

        /// <summary>
        /// Indica si la programación está activa
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Fecha y hora de creación de la programación
        /// </summary>
        public DateTime Created { get; set; }

        /// <summary>
        /// Fecha y hora de la última modificación
        /// </summary>
        public DateTime LastModified { get; set; }

        /// <summary>
        /// Fecha y hora de la última ejecución
        /// </summary>
        public DateTime? LastExecution { get; set; }

        /// <summary>
        /// Fecha y hora de la próxima ejecución programada
        /// </summary>
        public DateTime? NextExecution { get; set; }
    }
} 
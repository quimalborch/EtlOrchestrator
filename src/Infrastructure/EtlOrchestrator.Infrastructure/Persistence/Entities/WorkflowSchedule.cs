using System;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa una programación de ejecución de un flujo de trabajo
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
        /// Identificador del trabajo programado (usado por Hangfire)
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// Identificador del flujo de trabajo
        /// </summary>
        public string WorkflowId { get; set; }

        /// <summary>
        /// Expresión cron para la programación
        /// </summary>
        public string CronExpression { get; set; }

        /// <summary>
        /// Zona horaria para la programación
        /// </summary>
        public string TimeZone { get; set; }

        /// <summary>
        /// Fecha y hora de creación de la programación
        /// </summary>
        public DateTime Created { get; set; }

        /// <summary>
        /// Fecha y hora de la última modificación
        /// </summary>
        public DateTime LastModified { get; set; }

        /// <summary>
        /// Indica si la programación está habilitada
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Datos de entrada para la ejecución serializados como JSON
        /// </summary>
        public string InputDataJson { get; set; }

        /// <summary>
        /// Fecha y hora de la última ejecución
        /// </summary>
        public DateTime? LastExecution { get; set; }

        /// <summary>
        /// Fecha y hora de la próxima ejecución programada
        /// </summary>
        public DateTime? NextExecution { get; set; }

        /// <summary>
        /// Descripción opcional de la programación
        /// </summary>
        public string Description { get; set; }
    }
} 
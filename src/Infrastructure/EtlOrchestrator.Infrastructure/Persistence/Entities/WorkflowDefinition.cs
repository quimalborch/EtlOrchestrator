using System;
using System.Collections.Generic;

namespace EtlOrchestrator.Infrastructure.Persistence.Entities
{
    /// <summary>
    /// Entidad que representa la definición de un flujo de trabajo ETL
    /// </summary>
    public class WorkflowDefinition
    {
        /// <summary>
        /// Identificador único de la definición del flujo de trabajo
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Nombre del flujo de trabajo
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Versión del flujo de trabajo
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Descripción opcional del flujo de trabajo
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Fecha y hora de creación de la definición
        /// </summary>
        public DateTime Created { get; set; }

        /// <summary>
        /// Fecha y hora de la última modificación
        /// </summary>
        public DateTime LastModified { get; set; }

        /// <summary>
        /// Configuración del flujo de trabajo serializada como JSON
        /// Incluye información sobre conectores, transformaciones y carga
        /// </summary>
        public string ConfigurationJson { get; set; }

        /// <summary>
        /// Indica si esta definición de flujo de trabajo está activa
        /// </summary>
        public bool IsActive { get; set; } = true;

        /// <summary>
        /// Ejecuciones relacionadas con esta definición de flujo de trabajo
        /// </summary>
        public virtual ICollection<WorkflowExecution> Executions { get; set; } = new List<WorkflowExecution>();

        /// <summary>
        /// Programaciones relacionadas con esta definición de flujo de trabajo
        /// </summary>
        public virtual ICollection<WorkflowSchedule> Schedules { get; set; } = new List<WorkflowSchedule>();
    }
} 
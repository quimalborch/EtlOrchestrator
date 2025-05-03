using System;
using System.Collections.Generic;

namespace EtlOrchestrator.Core
{
    /// <summary>
    /// Contexto de ejecución para operaciones ETL
    /// </summary>
    public class Context
    {
        /// <summary>
        /// Identificador único del contexto
        /// </summary>
        public Guid Id { get; } = Guid.NewGuid();

        /// <summary>
        /// Nombre del job ETL
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// Identificador de la ejecución actual
        /// </summary>
        public string ExecutionId { get; set; }

        /// <summary>
        /// Marca de tiempo de inicio de la ejecución
        /// </summary>
        public DateTime StartTime { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// Parámetros de configuración para la ejecución
        /// </summary>
        public Dictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();

        /// <summary>
        /// Añade o actualiza un parámetro con el valor especificado
        /// </summary>
        public void SetParameter(string name, object value)
        {
            Parameters[name] = value;
        }

        /// <summary>
        /// Intenta obtener el valor de un parámetro
        /// </summary>
        public bool TryGetParameter<T>(string name, out T value)
        {
            if (Parameters.TryGetValue(name, out var objValue) && objValue is T typedValue)
            {
                value = typedValue;
                return true;
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Obtiene un parámetro con el tipo especificado, o el valor predeterminado si no existe
        /// </summary>
        public T GetParameter<T>(string name, T defaultValue = default)
        {
            if (TryGetParameter<T>(name, out var value))
            {
                return value;
            }
            return defaultValue;
        }
    }
} 
using System;
using EtlOrchestrator.Core;

namespace EtlOrchestrator.Infrastructure.Extensions
{
    /// <summary>
    /// Extensiones para la clase Record
    /// </summary>
    public static class RecordExtensions
    {
        /// <summary>
        /// Verifica si un registro tiene una propiedad
        /// </summary>
        /// <param name="record">Registro a verificar</param>
        /// <param name="name">Nombre de la propiedad</param>
        /// <returns>True si la propiedad existe, false en caso contrario</returns>
        public static bool HasProperty(this Record record, string name)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));
                
            return record.TryGetProperty(name, out _);
        }
        
        /// <summary>
        /// Obtiene el valor de una propiedad
        /// </summary>
        /// <param name="record">Registro del cual obtener la propiedad</param>
        /// <param name="name">Nombre de la propiedad</param>
        /// <returns>Valor de la propiedad o null si no existe</returns>
        public static object GetProperty(this Record record, string name)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));
                
            record.TryGetProperty(name, out var value);
            return value;
        }
    }
} 
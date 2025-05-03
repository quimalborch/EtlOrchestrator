using System;
using System.Collections.Generic;
using System.Dynamic;

namespace EtlOrchestrator.Core
{
    /// <summary>
    /// Representa un registro de datos genérico con propiedades dinámicas
    /// </summary>
    public class Record : DynamicObject
    {
        private readonly Dictionary<string, object> _properties = new Dictionary<string, object>();

        /// <summary>
        /// Identificador único del registro
        /// </summary>
        public Guid Id { get; } = Guid.NewGuid();

        /// <summary>
        /// Metadatos adicionales asociados al registro
        /// </summary>
        public Dictionary<string, object> Metadata { get; } = new Dictionary<string, object>();

        /// <summary>
        /// Obtiene o establece un valor utilizando indexación
        /// </summary>
        /// <param name="name">Nombre de la propiedad</param>
        /// <returns>Valor de la propiedad</returns>
        public object this[string name]
        {
            get => _properties.TryGetValue(name, out var value) ? value : null;
            set => _properties[name] = value;
        }

        /// <summary>
        /// Intenta obtener un miembro dinámico
        /// </summary>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return _properties.TryGetValue(binder.Name, out result);
        }

        /// <summary>
        /// Intenta establecer un miembro dinámico
        /// </summary>
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            _properties[binder.Name] = value;
            return true;
        }

        /// <summary>
        /// Obtiene todas las propiedades del registro
        /// </summary>
        public Dictionary<string, object> GetProperties()
        {
            return new Dictionary<string, object>(_properties);
        }

        /// <summary>
        /// Añade o actualiza una propiedad con el valor especificado
        /// </summary>
        public void SetProperty(string name, object value)
        {
            _properties[name] = value;
        }

        /// <summary>
        /// Intenta obtener el valor de una propiedad
        /// </summary>
        public bool TryGetProperty(string name, out object value)
        {
            return _properties.TryGetValue(name, out value);
        }
    }
} 
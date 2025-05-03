using System.Collections.Generic;
using System.Threading.Tasks;

namespace EtlOrchestrator.Core.Connectors
{
    /// <summary>
    /// Interfaz para conectores de carga de datos
    /// </summary>
    public interface ILoadConnector
    {
        /// <summary>
        /// Carga una colección de registros en un destino
        /// </summary>
        /// <param name="records">Colección de registros a cargar</param>
        /// <returns>Task que representa la operación asíncrona</returns>
        Task LoadAsync(IEnumerable<Record> records);
    }
} 
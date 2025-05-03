using System.Collections.Generic;
using System.Threading.Tasks;

namespace EtlOrchestrator.Core.Connectors
{
    /// <summary>
    /// Interfaz para transformadores de datos
    /// </summary>
    public interface ITransform
    {
        /// <summary>
        /// Transforma una colección de registros según reglas definidas
        /// </summary>
        /// <param name="records">Colección de registros a transformar</param>
        /// <returns>Colección de registros transformados</returns>
        Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records);
    }
} 
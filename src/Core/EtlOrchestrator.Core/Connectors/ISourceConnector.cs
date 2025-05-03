using System.Collections.Generic;
using System.Threading.Tasks;

namespace EtlOrchestrator.Core.Connectors
{
    /// <summary>
    /// Interfaz para todos los conectores de origen de datos
    /// </summary>
    public interface ISourceConnector
    {
        /// <summary>
        /// Extrae datos de la fuente y los devuelve como una colección de registros
        /// </summary>
        /// <param name="context">Contexto de extracción con parámetros de configuración</param>
        /// <returns>Colección de registros extraídos</returns>
        Task<IEnumerable<Record>> ExtractAsync(Context context);
    }
} 
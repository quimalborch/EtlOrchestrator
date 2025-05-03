using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ISourceConnector para extraer datos desde SQL Server utilizando Dapper
    /// </summary>
    public class SqlServerSourceConnector : ISourceConnector
    {
        private readonly ILogger<SqlServerSourceConnector> _logger;

        public SqlServerSourceConnector(ILogger<SqlServerSourceConnector> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Extrae datos de SQL Server según la consulta definida en el contexto
        /// </summary>
        /// <param name="context">Contexto con la configuración de la conexión y la consulta</param>
        /// <returns>Colección de registros extraídos</returns>
        public async Task<IEnumerable<Record>> ExtractAsync(Context context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            // Obtener parámetros del contexto
            if (!context.TryGetParameter<string>("ConnectionString", out var connectionString))
                throw new InvalidOperationException("El parámetro 'ConnectionString' es obligatorio");

            if (!context.TryGetParameter<string>("SqlQuery", out var sqlQuery))
                throw new InvalidOperationException("El parámetro 'SqlQuery' es obligatorio");

            object queryParams = null;
            context.TryGetParameter<object>("Parameters", out queryParams);

            _logger.LogInformation("Iniciando extracción desde SQL Server. Consulta: {Query}", sqlQuery);

            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    var result = await connection.QueryAsync(sqlQuery, queryParams);
                    
                    _logger.LogInformation("Extracción completada. Se obtuvieron {Count} registros", result.Count());
                    
                    // Convertir los resultados a Records
                    var records = result.Select(row => ConvertToRecord(row)).ToList();
                    return records;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de SQL Server: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Convierte una fila dinámica en un objeto Record
        /// </summary>
        private Record ConvertToRecord(dynamic dynamicRow)
        {
            var record = new Record();
            
            IDictionary<string, object> rowDict = (IDictionary<string, object>)dynamicRow;
            foreach (var property in rowDict)
            {
                record.SetProperty(property.Key, property.Value);
            }
            
            return record;
        }
    }
} 
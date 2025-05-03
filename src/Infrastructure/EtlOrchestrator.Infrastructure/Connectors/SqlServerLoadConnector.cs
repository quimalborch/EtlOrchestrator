using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ILoadConnector para cargar datos en SQL Server
    /// </summary>
    public class SqlServerLoadConnector : ILoadConnector
    {
        private readonly ILogger<SqlServerLoadConnector> _logger;

        public SqlServerLoadConnector(ILogger<SqlServerLoadConnector> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Carga los registros en una tabla de SQL Server
        /// </summary>
        /// <param name="records">Registros a cargar</param>
        /// <returns>Task que representa la operación asíncrona</returns>
        public async Task LoadAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            // Obtener parámetros del primer registro (asumimos que todos tienen los mismos metadatos)
            var firstRecord = records.FirstOrDefault();
            if (firstRecord == null)
            {
                _logger.LogWarning("No hay registros para cargar");
                return;
            }

            if (!firstRecord.Metadata.TryGetValue("ConnectionString", out var connectionStringObj) || 
                !(connectionStringObj is string connectionString))
            {
                throw new InvalidOperationException("El parámetro 'ConnectionString' es obligatorio en los metadatos del registro");
            }

            if (!firstRecord.Metadata.TryGetValue("TargetTable", out var targetTableObj) || 
                !(targetTableObj is string targetTable))
            {
                throw new InvalidOperationException("El parámetro 'TargetTable' es obligatorio en los metadatos del registro");
            }

            // Obtener estrategia opcional de carga
            var loadStrategy = firstRecord.Metadata.TryGetValue("LoadStrategy", out var strategyObj) 
                ? strategyObj.ToString() 
                : "BulkInsert";

            _logger.LogInformation("Iniciando carga de {Count} registros en {Table} usando estrategia {Strategy}", 
                records.Count(), targetTable, loadStrategy);

            try
            {
                switch (loadStrategy)
                {
                    case "BulkInsert":
                        await BulkInsertAsync(records, connectionString, targetTable);
                        break;
                    case "BatchInsert":
                        await BatchInsertAsync(records, connectionString, targetTable);
                        break;
                    case "MergeUpsert":
                        if (!firstRecord.Metadata.TryGetValue("KeyColumns", out var keyColumnsObj) || 
                            !(keyColumnsObj is IEnumerable<string> keyColumns))
                        {
                            throw new InvalidOperationException("El parámetro 'KeyColumns' es obligatorio para la estrategia MergeUpsert");
                        }
                        await MergeUpsertAsync(records, connectionString, targetTable, keyColumns as string[]);
                        break;
                    default:
                        throw new InvalidOperationException($"Estrategia de carga no soportada: {loadStrategy}");
                }

                _logger.LogInformation("Carga completada en {Table}", targetTable);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cargar datos en SQL Server: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Inserta registros utilizando SqlBulkCopy para máximo rendimiento
        /// </summary>
        private async Task BulkInsertAsync(IEnumerable<Record> records, string connectionString, string targetTable)
        {
            // Convertir los registros a un DataTable
            var dataTable = ConvertToDataTable(records);

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = targetTable;
                    
                    // Mapear las columnas del DataTable a las columnas de la tabla destino
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }
                    
                    // Opciones de configuración
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.EnableStreaming = true;
                    
                    // Ejecutar la operación de bulk insert
                    await bulkCopy.WriteToServerAsync(dataTable);
                }
            }
        }

        /// <summary>
        /// Inserta registros utilizando múltiples inserciones en lotes para mejor control de errores
        /// </summary>
        private async Task BatchInsertAsync(IEnumerable<Record> records, string connectionString, string targetTable)
        {
            // Agrupar registros en lotes para mejor rendimiento
            var recordsList = records.ToList();
            var batchSize = 100;
            var totalBatches = (int)Math.Ceiling(recordsList.Count / (double)batchSize);
            
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                
                for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++)
                {
                    var batch = recordsList.Skip(batchIndex * batchSize).Take(batchSize).ToList();
                    
                    if (batch.Count == 0)
                        continue;
                    
                    // Construir consulta SQL para inserción múltiple
                    var insertSql = BuildBatchInsertSql(batch, targetTable);
                    
                    // Ejecutar la consulta
                    await connection.ExecuteAsync(insertSql.Query, insertSql.Parameters);
                    
                    _logger.LogDebug("Insertado lote {BatchIndex}/{TotalBatches} con {Count} registros", 
                        batchIndex + 1, totalBatches, batch.Count);
                }
            }
        }

        /// <summary>
        /// Inserta o actualiza registros utilizando la sentencia MERGE de SQL Server
        /// </summary>
        private async Task MergeUpsertAsync(IEnumerable<Record> records, string connectionString, string targetTable, string[] keyColumns)
        {
            // Utilizamos un enfoque similar al de BatchInsertAsync pero con sentencias MERGE
            var recordsList = records.ToList();
            var batchSize = 50; // MERGE es más costoso, usamos lotes más pequeños
            var totalBatches = (int)Math.Ceiling(recordsList.Count / (double)batchSize);
            
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                
                for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++)
                {
                    var batch = recordsList.Skip(batchIndex * batchSize).Take(batchSize).ToList();
                    
                    if (batch.Count == 0)
                        continue;
                    
                    // Construir consulta SQL para MERGE
                    var mergeSql = BuildMergeSql(batch, targetTable, keyColumns);
                    
                    // Ejecutar la consulta
                    await connection.ExecuteAsync(mergeSql.Query, mergeSql.Parameters);
                    
                    _logger.LogDebug("Procesado lote MERGE {BatchIndex}/{TotalBatches} con {Count} registros", 
                        batchIndex + 1, totalBatches, batch.Count);
                }
            }
        }

        /// <summary>
        /// Convierte los registros a un DataTable para usar con SqlBulkCopy
        /// </summary>
        private DataTable ConvertToDataTable(IEnumerable<Record> records)
        {
            var dataTable = new DataTable();
            var recordsList = records.ToList();
            
            if (recordsList.Count == 0)
                return dataTable;
            
            // Obtener el conjunto de todas las propiedades de los registros
            var allProperties = new HashSet<string>();
            foreach (var record in recordsList)
            {
                foreach (var property in record.GetProperties())
                {
                    allProperties.Add(property.Key);
                }
            }
            
            // Crear columnas en el DataTable
            foreach (var propertyName in allProperties)
            {
                dataTable.Columns.Add(propertyName, typeof(object));
            }
            
            // Añadir filas al DataTable
            foreach (var record in recordsList)
            {
                var row = dataTable.NewRow();
                var properties = record.GetProperties();
                
                foreach (var property in properties)
                {
                    if (dataTable.Columns.Contains(property.Key))
                    {
                        row[property.Key] = property.Value ?? DBNull.Value;
                    }
                }
                
                dataTable.Rows.Add(row);
            }
            
            return dataTable;
        }

        /// <summary>
        /// Construye una consulta SQL para la inserción en lotes
        /// </summary>
        private (string Query, DynamicParameters Parameters) BuildBatchInsertSql(List<Record> batch, string targetTable)
        {
            // Obtener el conjunto de todas las propiedades de los registros
            var allProperties = new HashSet<string>();
            foreach (var record in batch)
            {
                foreach (var property in record.GetProperties())
                {
                    allProperties.Add(property.Key);
                }
            }
            
            var columnList = string.Join(", ", allProperties);
            var parameters = new DynamicParameters();
            var queryBuilder = new StringBuilder();
            
            queryBuilder.AppendLine($"INSERT INTO {targetTable} ({columnList}) VALUES ");
            
            for (int i = 0; i < batch.Count; i++)
            {
                queryBuilder.Append("(");
                var record = batch[i];
                var values = new List<string>();
                
                foreach (var propertyName in allProperties)
                {
                    var paramName = $"@p{i}_{propertyName}";
                    
                    if (record.TryGetProperty(propertyName, out var value))
                    {
                        parameters.Add(paramName, value);
                    }
                    else
                    {
                        parameters.Add(paramName, null);
                    }
                    
                    values.Add(paramName);
                }
                
                queryBuilder.Append(string.Join(", ", values));
                queryBuilder.Append(i < batch.Count - 1 ? "), " : ")");
            }
            
            return (queryBuilder.ToString(), parameters);
        }

        /// <summary>
        /// Construye una consulta SQL con MERGE para actualizar o insertar registros
        /// </summary>
        private (string Query, DynamicParameters Parameters) BuildMergeSql(List<Record> batch, string targetTable, string[] keyColumns)
        {
            // Crear una tabla temporal para almacenar los datos de origen
            var allProperties = new HashSet<string>();
            foreach (var record in batch)
            {
                foreach (var property in record.GetProperties())
                {
                    allProperties.Add(property.Key);
                }
            }
            
            var parameters = new DynamicParameters();
            var queryBuilder = new StringBuilder();
            var tempTableName = $"#TempTable_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            
            // Crear tabla temporal
            queryBuilder.AppendLine($"CREATE TABLE {tempTableName} (");
            queryBuilder.AppendLine(string.Join(", ", allProperties.Select(p => $"[{p}] NVARCHAR(MAX)")));
            queryBuilder.AppendLine(");");
            
            // Insertar en tabla temporal
            foreach (var record in batch)
            {
                var insertColumns = new List<string>();
                var insertParams = new List<string>();
                var recordId = Guid.NewGuid().ToString("N");
                
                foreach (var property in allProperties)
                {
                    var paramName = $"@src_{recordId}_{property}";
                    insertColumns.Add($"[{property}]");
                    insertParams.Add(paramName);
                    
                    if (record.TryGetProperty(property, out var value))
                    {
                        parameters.Add(paramName, value?.ToString());
                    }
                    else
                    {
                        parameters.Add(paramName, null);
                    }
                }
                
                queryBuilder.AppendLine($"INSERT INTO {tempTableName} ({string.Join(", ", insertColumns)}) VALUES ({string.Join(", ", insertParams)});");
            }
            
            // Construir cláusula MERGE
            queryBuilder.AppendLine($"MERGE {targetTable} AS TARGET");
            queryBuilder.AppendLine($"USING {tempTableName} AS SOURCE");
            
            // ON con claves primarias
            queryBuilder.Append("ON (");
            queryBuilder.Append(string.Join(" AND ", keyColumns.Select(k => $"TARGET.[{k}] = SOURCE.[{k}]")));
            queryBuilder.AppendLine(")");
            
            // WHEN MATCHED UPDATE
            var updateColumns = allProperties.Except(keyColumns).ToList();
            if (updateColumns.Count > 0)
            {
                queryBuilder.AppendLine("WHEN MATCHED THEN");
                queryBuilder.Append("UPDATE SET ");
                queryBuilder.AppendLine(string.Join(", ", updateColumns.Select(c => $"TARGET.[{c}] = SOURCE.[{c}]")));
            }
            
            // WHEN NOT MATCHED INSERT
            queryBuilder.AppendLine("WHEN NOT MATCHED THEN");
            queryBuilder.Append($"INSERT ({string.Join(", ", allProperties.Select(p => $"[{p}]"))}) ");
            queryBuilder.AppendLine($"VALUES ({string.Join(", ", allProperties.Select(p => $"SOURCE.[{p}]"))});");
            
            // Eliminar tabla temporal
            queryBuilder.AppendLine($"DROP TABLE {tempTableName};");
            
            return (queryBuilder.ToString(), parameters);
        }
    }
} 
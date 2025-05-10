using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ITransform para formatear y manipular fechas y horas
    /// </summary>
    public class DateTimeFormatTransform : ITransform
    {
        private readonly ILogger<DateTimeFormatTransform> _logger;
        private readonly Dictionary<string, Func<DateTime, Dictionary<string, object>, object>> _dateOperations;

        public DateTimeFormatTransform(ILogger<DateTimeFormatTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Inicializar operaciones de fecha/hora predefinidas
            _dateOperations = new Dictionary<string, Func<DateTime, Dictionary<string, object>, object>>
            {
                // Formatear fecha/hora en un string
                { "Format", (date, parameters) => {
                    if (!parameters.TryGetValue("Format", out var formatObj) || !(formatObj is string format))
                        format = "yyyy-MM-dd HH:mm:ss";
                    
                    CultureInfo culture = CultureInfo.InvariantCulture;
                    if (parameters.TryGetValue("Culture", out var cultureObj) && cultureObj is string cultureName)
                    {
                        try
                        {
                            culture = new CultureInfo(cultureName);
                        }
                        catch
                        {
                            _logger.LogWarning("Cultura inválida: {Culture}, usando InvariantCulture", cultureName);
                        }
                    }
                    
                    return date.ToString(format, culture);
                }},
                
                // Extraer año
                { "Year", (date, _) => date.Year },
                
                // Extraer mes
                { "Month", (date, _) => date.Month },
                
                // Extraer día
                { "Day", (date, _) => date.Day },
                
                // Extraer hora
                { "Hour", (date, _) => date.Hour },
                
                // Extraer minuto
                { "Minute", (date, _) => date.Minute },
                
                // Extraer segundo
                { "Second", (date, _) => date.Second },
                
                // Día de la semana (nombre)
                { "DayOfWeek", (date, parameters) => {
                    CultureInfo culture = CultureInfo.InvariantCulture;
                    if (parameters.TryGetValue("Culture", out var cultureObj) && cultureObj is string cultureName)
                    {
                        try
                        {
                            culture = new CultureInfo(cultureName);
                        }
                        catch
                        {
                            _logger.LogWarning("Cultura inválida: {Culture}, usando InvariantCulture", cultureName);
                        }
                    }
                    
                    return culture.DateTimeFormat.GetDayName(date.DayOfWeek);
                }},
                
                // Mes del año (nombre)
                { "MonthName", (date, parameters) => {
                    CultureInfo culture = CultureInfo.InvariantCulture;
                    if (parameters.TryGetValue("Culture", out var cultureObj) && cultureObj is string cultureName)
                    {
                        try
                        {
                            culture = new CultureInfo(cultureName);
                        }
                        catch
                        {
                            _logger.LogWarning("Cultura inválida: {Culture}, usando InvariantCulture", cultureName);
                        }
                    }
                    
                    return culture.DateTimeFormat.GetMonthName(date.Month);
                }},
                
                // Trimestre
                { "Quarter", (date, _) => (date.Month - 1) / 3 + 1 },
                
                // Día del año
                { "DayOfYear", (date, _) => date.DayOfYear },
                
                // Semana del año
                { "WeekOfYear", (date, parameters) => {
                    CultureInfo culture = CultureInfo.InvariantCulture;
                    if (parameters.TryGetValue("Culture", out var cultureObj) && cultureObj is string cultureName)
                    {
                        try
                        {
                            culture = new CultureInfo(cultureName);
                        }
                        catch
                        {
                            _logger.LogWarning("Cultura inválida: {Culture}, usando InvariantCulture", cultureName);
                        }
                    }
                    
                    Calendar calendar = culture.Calendar;
                    CalendarWeekRule weekRule = parameters.TryGetValue("WeekRule", out var weekRuleObj) && 
                                              Enum.TryParse<CalendarWeekRule>(weekRuleObj?.ToString(), out var rule)
                                                ? rule
                                                : CalendarWeekRule.FirstDay;
                                                
                    DayOfWeek firstDayOfWeek = parameters.TryGetValue("FirstDayOfWeek", out var firstDayObj) && 
                                             Enum.TryParse<DayOfWeek>(firstDayObj?.ToString(), out var firstDay)
                                                ? firstDay
                                                : DayOfWeek.Sunday;
                    
                    return calendar.GetWeekOfYear(date, weekRule, firstDayOfWeek);
                }},
                
                // Unix timestamp (segundos desde 1970-01-01)
                { "UnixTimestamp", (date, _) => {
                    var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    return (long)(date.ToUniversalTime() - epoch).TotalSeconds;
                }},
                
                // Calcular edad en años
                { "Age", (date, _) => {
                    var today = DateTime.Today;
                    int age = today.Year - date.Year;
                    if (date.Date > today.AddYears(-age)) age--;
                    return age;
                }},
                
                // Extraer solo la fecha (sin hora)
                { "DateOnly", (date, _) => date.Date },
                
                // Extraer solo la hora (sin fecha)
                { "TimeOnly", (date, _) => date.TimeOfDay },
                
                // Añadir intervalo de tiempo
                { "Add", (date, parameters) => {
                    var result = date;
                    
                    if (parameters.TryGetValue("Years", out var yearsObj) && 
                        int.TryParse(yearsObj?.ToString(), out var years))
                    {
                        result = result.AddYears(years);
                    }
                    
                    if (parameters.TryGetValue("Months", out var monthsObj) && 
                        int.TryParse(monthsObj?.ToString(), out var months))
                    {
                        result = result.AddMonths(months);
                    }
                    
                    if (parameters.TryGetValue("Days", out var daysObj) && 
                        int.TryParse(daysObj?.ToString(), out var days))
                    {
                        result = result.AddDays(days);
                    }
                    
                    if (parameters.TryGetValue("Hours", out var hoursObj) && 
                        int.TryParse(hoursObj?.ToString(), out var hours))
                    {
                        result = result.AddHours(hours);
                    }
                    
                    if (parameters.TryGetValue("Minutes", out var minutesObj) && 
                        int.TryParse(minutesObj?.ToString(), out var minutes))
                    {
                        result = result.AddMinutes(minutes);
                    }
                    
                    if (parameters.TryGetValue("Seconds", out var secondsObj) && 
                        int.TryParse(secondsObj?.ToString(), out var seconds))
                    {
                        result = result.AddSeconds(seconds);
                    }
                    
                    return result;
                }},
                
                // Truncar fecha/hora a un nivel específico
                { "Truncate", (date, parameters) => {
                    if (!parameters.TryGetValue("Level", out var levelObj) || !(levelObj is string level))
                        level = "Day";
                    
                    switch (level.ToLower())
                    {
                        case "year":
                            return new DateTime(date.Year, 1, 1, 0, 0, 0);
                        case "month":
                            return new DateTime(date.Year, date.Month, 1, 0, 0, 0);
                        case "day":
                            return new DateTime(date.Year, date.Month, date.Day, 0, 0, 0);
                        case "hour":
                            return new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0);
                        case "minute":
                            return new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, 0);
                        default:
                            return date;
                    }
                }}
            };
        }

        /// <summary>
        /// Aplica transformaciones de fecha/hora según la configuración
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando formato de fecha/hora para {Count} registros", recordsList.Count);

            // Verificar si hay configuración de fecha/hora
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("DateTimeConfig"))
            {
                _logger.LogWarning("No se encontró configuración de fecha/hora en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración de fecha/hora
                var dateTimeConfig = firstRecord.Metadata["DateTimeConfig"] as Dictionary<string, object>;
                if (dateTimeConfig == null)
                {
                    _logger.LogWarning("El formato de configuración de fecha/hora es inválido");
                    return recordsList;
                }

                // Obtener operaciones de fecha/hora
                if (!dateTimeConfig.TryGetValue("Operations", out var operationsObj) || 
                    !(operationsObj is Dictionary<string, Dictionary<string, object>> operations) ||
                    operations.Count == 0)
                {
                    _logger.LogWarning("No se encontraron operaciones de fecha/hora válidas");
                    return recordsList;
                }

                // Procesar cada registro
                var transformedRecords = new List<Record>();
                await Task.Run(() =>
                {
                    foreach (var record in recordsList)
                    {
                        try
                        {
                            var transformedRecord = ApplyDateTimeOperations(record, operations);
                            transformedRecords.Add(transformedRecord);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error procesando fecha/hora en registro: {Message}", ex.Message);
                            
                            // Determinar si incluir registros con error
                            bool includeOnError = dateTimeConfig.TryGetValue("IncludeOnError", out var includeObj) &&
                                               includeObj is bool includeValue && includeValue;
                                               
                            if (includeOnError)
                            {
                                // Marcar el registro con error y agregarlo
                                record.Metadata["DateTimeFormatError"] = ex.Message;
                                transformedRecords.Add(record);
                            }
                        }
                    }
                });

                _logger.LogInformation("Formato de fecha/hora completado para {Count} registros", transformedRecords.Count);
                return transformedRecords;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante el formato de fecha/hora: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Aplica operaciones de fecha/hora a un registro
        /// </summary>
        private Record ApplyDateTimeOperations(
            Record record,
            Dictionary<string, Dictionary<string, object>> operations)
        {
            // Crear nuevo registro con los mismos metadatos
            var result = new Record();
            foreach (var meta in record.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }
            
            // Copiar todas las propiedades inicialmente
            foreach (var property in record.GetProperties())
            {
                result.SetProperty(property.Key, property.Value);
            }
            
            // Aplicar operaciones de fecha/hora
            foreach (var operation in operations)
            {
                var fieldName = operation.Key;
                var operationDetails = operation.Value;
                
                // Verificar si el registro tiene la propiedad
                if (!record.HasProperty(fieldName))
                {
                    continue;
                }
                
                // Obtener valor actual y verificar si es una fecha válida
                var value = record.GetProperty(fieldName);
                if (value == null || !DateTime.TryParse(value.ToString(), out var dateValue))
                {
                    // Verificar formatos personalizados
                    if (operationDetails.TryGetValue("InputFormat", out var inputFormatObj) && 
                        inputFormatObj is string inputFormat)
                    {
                        CultureInfo culture = CultureInfo.InvariantCulture;
                        if (operationDetails.TryGetValue("InputCulture", out var cultureObj) && 
                            cultureObj is string cultureName)
                        {
                            try
                            {
                                culture = new CultureInfo(cultureName);
                            }
                            catch
                            {
                                _logger.LogWarning("Cultura inválida: {Culture}, usando InvariantCulture", cultureName);
                            }
                        }
                        
                        if (value != null && DateTime.TryParseExact(value.ToString(), inputFormat, culture,
                                                       DateTimeStyles.None, out dateValue))
                        {
                            // Formato válido, continuar con el procesamiento
                        }
                        else
                        {
                            // No se pudo parsear la fecha/hora
                            _logger.LogWarning("No se pudo parsear la fecha/hora: {Value}", value);
                            continue;
                        }
                    }
                    else
                    {
                        // No se pudo parsear la fecha/hora y no hay formato personalizado
                        _logger.LogWarning("No se pudo parsear la fecha/hora: {Value}", value);
                        continue;
                    }
                }
                
                // Obtener operación a realizar
                if (!operationDetails.TryGetValue("Operation", out var operationTypeObj) || 
                    !(operationTypeObj is string operationType))
                {
                    _logger.LogWarning("Operación no especificada para el campo {Field}", fieldName);
                    continue;
                }
                
                // Verificar si se debe crear un nuevo campo
                bool createNewField = operationDetails.TryGetValue("CreateNewField", out var createNewObj) && 
                                    createNewObj is bool createNew && createNew;
                                      
                string targetField = fieldName;
                if (createNewField && operationDetails.TryGetValue("NewFieldName", out var newFieldObj) && 
                    newFieldObj is string newField)
                {
                    targetField = newField;
                }
                
                // Aplicar la operación
                if (_dateOperations.TryGetValue(operationType, out var dateFunc))
                {
                    // Extraer parámetros de operación
                    var parameters = new Dictionary<string, object>();
                    if (operationDetails.TryGetValue("Parameters", out var paramsObj) && 
                        paramsObj is Dictionary<string, object> paramsDict)
                    {
                        foreach (var param in paramsDict)
                        {
                            parameters[param.Key] = param.Value;
                        }
                    }
                    
                    // Ejecutar operación
                    var processedValue = dateFunc(dateValue, parameters);
                    result.SetProperty(targetField, processedValue);
                }
                else
                {
                    _logger.LogWarning("Operación de fecha/hora no soportada: {Operation}", operationType);
                }
            }
            
            // Agregar metadatos sobre la operación
            result.Metadata["DateTimeFormatApplied"] = true;
            
            return result;
        }

        /// <summary>
        /// Agrega una operación de fecha/hora personalizada
        /// </summary>
        public void AddDateTimeOperation(string name, Func<DateTime, Dictionary<string, object>, object> operation)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (operation == null)
                throw new ArgumentNullException(nameof(operation));
                
            _dateOperations[name] = operation;
        }
    }
} 
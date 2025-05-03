using System;
using Microsoft.EntityFrameworkCore;
using EtlOrchestrator.Infrastructure.Persistence.Entities;

namespace EtlOrchestrator.Infrastructure.Persistence
{
    /// <summary>
    /// Contexto de base de datos para el orquestador ETL
    /// </summary>
    public class EtlOrchestratorDbContext : DbContext
    {
        public EtlOrchestratorDbContext(DbContextOptions<EtlOrchestratorDbContext> options)
            : base(options)
        {
        }

        /// <summary>
        /// Definiciones de flujos de trabajo
        /// </summary>
        public DbSet<WorkflowDefinition> WorkflowDefinitions { get; set; }

        /// <summary>
        /// Ejecuciones de flujos de trabajo
        /// </summary>
        public DbSet<WorkflowExecution> WorkflowExecutions { get; set; }

        /// <summary>
        /// Pasos de ejecución de los flujos de trabajo
        /// </summary>
        public DbSet<WorkflowExecutionStep> WorkflowExecutionSteps { get; set; }

        /// <summary>
        /// Logs de ejecución
        /// </summary>
        public DbSet<WorkflowLog> WorkflowLogs { get; set; }

        /// <summary>
        /// Programaciones de flujos de trabajo
        /// </summary>
        public DbSet<WorkflowSchedule> WorkflowSchedules { get; set; }

        /// <summary>
        /// Configuración del modelo
        /// </summary>
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            
            // Configurar la entidad WorkflowDefinition
            modelBuilder.Entity<WorkflowDefinition>(entity =>
            {
                entity.ToTable("WorkflowDefinitions");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Name).IsRequired().HasMaxLength(100);
                entity.Property(e => e.Version).IsRequired();
                entity.Property(e => e.Description).HasMaxLength(500);
                entity.Property(e => e.Created).IsRequired();
                entity.Property(e => e.LastModified).IsRequired();
                entity.Property(e => e.ConfigurationJson).IsRequired();
                
                // Índice compuesto para buscar por nombre y versión
                entity.HasIndex(e => new { e.Name, e.Version }).IsUnique();
            });
            
            // Configurar la entidad WorkflowExecution
            modelBuilder.Entity<WorkflowExecution>(entity =>
            {
                entity.ToTable("WorkflowExecutions");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.WorkflowId).IsRequired().HasMaxLength(100);
                entity.Property(e => e.InstanceId).IsRequired().HasMaxLength(100);
                entity.Property(e => e.Status).IsRequired().HasMaxLength(50);
                entity.Property(e => e.StartTime).IsRequired();
                entity.Property(e => e.EndTime);
                entity.Property(e => e.InputDataJson);
                entity.Property(e => e.OutputDataJson);
                entity.Property(e => e.ErrorMessage).HasMaxLength(2000);
                
                // Índice para buscar por WorkflowId e InstanceId
                entity.HasIndex(e => e.WorkflowId);
                entity.HasIndex(e => e.InstanceId);
                
                // Relación con WorkflowDefinition
                entity.HasOne(e => e.WorkflowDefinition)
                      .WithMany(d => d.Executions)
                      .HasForeignKey(e => e.WorkflowDefinitionId)
                      .OnDelete(DeleteBehavior.Restrict);
            });
            
            // Configurar la entidad WorkflowExecutionStep
            modelBuilder.Entity<WorkflowExecutionStep>(entity =>
            {
                entity.ToTable("WorkflowExecutionSteps");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.StepName).IsRequired().HasMaxLength(100);
                entity.Property(e => e.StepType).IsRequired().HasMaxLength(200);
                entity.Property(e => e.Status).IsRequired().HasMaxLength(50);
                entity.Property(e => e.StartTime).IsRequired();
                entity.Property(e => e.EndTime);
                entity.Property(e => e.InputDataJson);
                entity.Property(e => e.OutputDataJson);
                entity.Property(e => e.ErrorMessage).HasMaxLength(2000);
                
                // Índice para buscar por ExecutionId
                entity.HasIndex(e => e.WorkflowExecutionId);
                
                // Relación con WorkflowExecution
                entity.HasOne(e => e.WorkflowExecution)
                      .WithMany(d => d.Steps)
                      .HasForeignKey(e => e.WorkflowExecutionId)
                      .OnDelete(DeleteBehavior.Cascade);
            });
            
            // Configurar la entidad WorkflowLog
            modelBuilder.Entity<WorkflowLog>(entity =>
            {
                entity.ToTable("WorkflowLogs");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Timestamp).IsRequired();
                entity.Property(e => e.LogLevel).IsRequired().HasMaxLength(20);
                entity.Property(e => e.Category).HasMaxLength(100);
                entity.Property(e => e.Message).IsRequired();
                entity.Property(e => e.WorkflowId).HasMaxLength(100);
                entity.Property(e => e.InstanceId).HasMaxLength(100);
                entity.Property(e => e.StepName).HasMaxLength(100);
                
                // Índices para consultas comunes
                entity.HasIndex(e => e.Timestamp);
                entity.HasIndex(e => e.WorkflowId);
                entity.HasIndex(e => e.InstanceId);
            });
            
            // Configurar la entidad WorkflowSchedule
            modelBuilder.Entity<WorkflowSchedule>(entity =>
            {
                entity.ToTable("WorkflowSchedules");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.JobId).IsRequired().HasMaxLength(100);
                entity.Property(e => e.WorkflowId).IsRequired().HasMaxLength(100);
                entity.Property(e => e.CronExpression).IsRequired().HasMaxLength(100);
                entity.Property(e => e.TimeZone).IsRequired().HasMaxLength(50);
                entity.Property(e => e.Created).IsRequired();
                entity.Property(e => e.LastModified).IsRequired();
                entity.Property(e => e.Enabled).IsRequired();
                entity.Property(e => e.InputDataJson);
                entity.Property(e => e.Description).HasMaxLength(500);
                
                // Índice para JobId
                entity.HasIndex(e => e.JobId).IsUnique();
                
                // Relación con WorkflowDefinition
                entity.HasOne(e => e.WorkflowDefinition)
                      .WithMany(d => d.Schedules)
                      .HasForeignKey(e => e.WorkflowDefinitionId)
                      .OnDelete(DeleteBehavior.Restrict);
            });
        }
    }
} 
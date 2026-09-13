using System.Text.Json;
using Confluent.Kafka;
using ReportingService.Messaging.Contracts;
using ReportingService.Models;
using ReportingService.Repositories;

namespace ReportingService.Messaging.Consumers;

/// <summary>Projects Dispatch JobAssigned events for Jobs-by-Technician reports.</summary>
public sealed class JobAssignedConsumer : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
    private readonly ConsumerConfig _config;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JobAssignedConsumer> _logger;

    public JobAssignedConsumer(string bootstrapServers, IServiceScopeFactory scopeFactory, ILogger<JobAssignedConsumer> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = JobAssignedPayload.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();
        consumer.Subscribe(JobAssignedPayload.Topic);
        _logger.LogInformation("Subscribed to {Topic} as group {GroupId}.", JobAssignedPayload.Topic, JobAssignedPayload.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> message;
                try { message = consumer.Consume(stoppingToken); }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consuming from {Topic} failed.", JobAssignedPayload.Topic);
                    continue;
                }

                EventEnvelope<JobAssignedPayload>? envelope;
                try { envelope = JsonSerializer.Deserialize<EventEnvelope<JobAssignedPayload>>(message.Message.Value, SerializerOptions); }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Discarding malformed JobAssigned event at {Offset}.", message.TopicPartitionOffset);
                    consumer.Commit(message);
                    continue;
                }

                if (envelope?.Payload is null || string.IsNullOrWhiteSpace(envelope.Payload.JobId) ||
                    string.IsNullOrWhiteSpace(envelope.Payload.TechnicianId) || string.IsNullOrWhiteSpace(envelope.Payload.TechnicianReference))
                {
                    _logger.LogError("Discarding invalid JobAssigned event at {Offset}.", message.TopicPartitionOffset);
                    consumer.Commit(message);
                    continue;
                }

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJobProjectionRepository>();
                    await repository.ApplyAssignmentAsync(new JobAssignmentProjection
                    {
                        JobId = envelope.Payload.JobId,
                        TechnicianId = envelope.Payload.TechnicianId,
                        TechnicianReference = envelope.Payload.TechnicianReference,
                        AssignedAt = envelope.Payload.AssignedAt,
                    });
                    consumer.Commit(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Projecting JobAssigned at {Offset} failed. It will be retried.", message.TopicPartitionOffset);
                    consumer.Seek(message.TopicPartitionOffset);
                    await Task.Delay(RetryDelay, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Stopping the {Topic} consumer.", JobAssignedPayload.Topic);
        }
        finally { consumer.Close(); }
    }
}

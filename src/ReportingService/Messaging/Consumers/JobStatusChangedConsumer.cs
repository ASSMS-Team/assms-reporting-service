using System.Text.Json;
using Confluent.Kafka;
using ReportingService.Messaging.Contracts;
using ReportingService.Models;
using ReportingService.Repositories;

namespace ReportingService.Messaging.Consumers;

/// <summary>Projects JobService JobStatusChanged events for Job Completion reports and status projections.</summary>
public sealed class JobStatusChangedConsumer : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
    private readonly ConsumerConfig _config;
    private readonly Func<ConsumerConfig, IConsumer<string, string>> _consumerFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JobStatusChangedConsumer> _logger;

    public JobStatusChangedConsumer(string bootstrapServers, IServiceScopeFactory scopeFactory, ILogger<JobStatusChangedConsumer> logger)
        : this(bootstrapServers, scopeFactory, logger, config => new ConsumerBuilder<string, string>(config).Build())
    {
    }

    internal JobStatusChangedConsumer(
        string bootstrapServers,
        IServiceScopeFactory scopeFactory,
        ILogger<JobStatusChangedConsumer> logger,
        Func<ConsumerConfig, IConsumer<string, string>> consumerFactory)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _consumerFactory = consumerFactory;
        _config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = JobStatusChangedPayload.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        using var consumer = _consumerFactory(_config);
        consumer.Subscribe(JobStatusChangedPayload.Topic);
        _logger.LogInformation("Subscribed to {Topic} as group {GroupId}.", JobStatusChangedPayload.Topic, JobStatusChangedPayload.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> message;
                try { message = consumer.Consume(stoppingToken); }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consuming from {Topic} failed.", JobStatusChangedPayload.Topic);
                    continue;
                }

                EventEnvelope<JobStatusChangedPayload>? envelope;
                try { envelope = Deserialize(message.Message.Value); }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Discarding malformed JobStatusChanged event at {Offset}.", message.TopicPartitionOffset);
                    consumer.Commit(message);
                    continue;
                }

                if (!IsUsable(envelope, message.Message.Key))
                {
                    _logger.LogError("Discarding invalid JobStatusChanged event at {Offset}.", message.TopicPartitionOffset);
                    consumer.Commit(message);
                    continue;
                }

                var payload = envelope!.Payload;

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJobProjectionRepository>();
                    await repository.ApplyJobStatusChangedAsync(new JobStatusChangeProjection
                    {
                        JobId = payload.JobId,
                        JobReference = payload.JobReference,
                        TechnicianId = payload.TechnicianId,
                        TechnicianReference = payload.TechnicianReference,
                        OldStatus = payload.OldStatus,
                        NewStatus = payload.NewStatus,
                        OccurredAt = payload.OccurredAt,
                    });
                    consumer.Commit(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Projecting JobStatusChanged at {Offset} failed. It will be retried.", message.TopicPartitionOffset);
                    consumer.Seek(message.TopicPartitionOffset);
                    await Task.Delay(RetryDelay, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Stopping the {Topic} consumer.", JobStatusChangedPayload.Topic);
        }
        finally { consumer.Close(); }
    }

    internal static EventEnvelope<JobStatusChangedPayload>? Deserialize(string value) =>
        JsonSerializer.Deserialize<EventEnvelope<JobStatusChangedPayload>>(value, SerializerOptions);

    internal static bool IsUsable(EventEnvelope<JobStatusChangedPayload>? envelope, string? messageKey) =>
        envelope?.Payload is not null
        && Guid.TryParse(envelope.EventId, out _)
        && string.Equals(envelope.EventType, JobStatusChangedPayload.EventType, StringComparison.Ordinal)
        && envelope.EventVersion == JobStatusChangedPayload.EventVersion
        && string.Equals(envelope.Producer, JobStatusChangedPayload.Producer, StringComparison.Ordinal)
        && envelope.OccurredAt != default
        && envelope.OccurredAt.Kind == DateTimeKind.Utc
        && Guid.TryParse(envelope.Payload.JobId, out _)
        && !string.IsNullOrWhiteSpace(envelope.Payload.JobReference)
        && !string.IsNullOrWhiteSpace(envelope.Payload.OldStatus)
        && !string.IsNullOrWhiteSpace(envelope.Payload.NewStatus)
        && envelope.Payload.OccurredAt != default
        && envelope.Payload.OccurredAt.Kind == DateTimeKind.Utc
        && string.Equals(messageKey, envelope.Payload.JobId, StringComparison.Ordinal);
}

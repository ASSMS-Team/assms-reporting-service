using System.Text.Json;

using Confluent.Kafka;

using ReportingService.Messaging.Contracts;
using ReportingService.Models;
using ReportingService.Repositories;

namespace ReportingService.Messaging.Consumers;

// Reads job-created and keeps the job_projection read model up to date.
//
// A BackgroundService rather than anything triggered by a request: the events
// arrive whether or not anyone is looking at the report, and the projection has
// to already be current when the first request comes in, not be built during it.
public class JobCreatedConsumer : BackgroundService
{
    // How long to wait before re-reading a message whose write failed. Without
    // it a database that is down turns the retry into a hot loop that reopens a
    // connection thousands of times a second and fills the log with one line.
    private static readonly TimeSpan WriteRetryDelay = TimeSpan.FromSeconds(5);

    // camelCase, mirroring how the Job Service serializes the envelope. The
    // policy applies on the way in as well as out, so this is what maps the
    // published "jobId" onto JobId.
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly ConsumerConfig _consumerConfig;
    private readonly Func<ConsumerConfig, IConsumer<string, string>> _consumerFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JobCreatedConsumer> _logger;

    public JobCreatedConsumer(
        string bootstrapServers,
        IServiceScopeFactory scopeFactory,
        ILogger<JobCreatedConsumer> logger)
        : this(
            bootstrapServers,
            scopeFactory,
            logger,
            config => new ConsumerBuilder<string, string>(config).Build())
    {
    }

    // The factory exists so a test can hand the loop a consumer it controls.
    // Building one inside ExecuteAsync would tie the whole of this class to a
    // running broker, and the branch worth testing - commit past the message or
    // seek back to it - is exactly the branch that would then be unreachable.
    internal JobCreatedConsumer(
        string bootstrapServers,
        IServiceScopeFactory scopeFactory,
        ILogger<JobCreatedConsumer> logger,
        Func<ConsumerConfig, IConsumer<string, string>> consumerFactory)
    {
        _consumerFactory = consumerFactory;
        _scopeFactory = scopeFactory;
        _logger = logger;

        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = JobCreatedPayload.ConsumerGroup,
            // The offset is committed by this code, after the row is written.
            // Auto-commit moves it on a timer with no idea whether the write
            // succeeded, so a crash between the timer firing and the write
            // would lose that job from the report permanently - and a report
            // quietly missing rows is worse than one that is late.
            EnableAutoCommit = false,
            // A group reading a topic for the first time starts at the
            // beginning, so a projection built after jobs already exist catches
            // up on them rather than starting blank. Latest would silently
            // under-report every job raised before this service first ran.
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ExecuteAsync runs inline on the host's startup path until its first
        // await, and Consume blocks the thread it is called on. Yielding here
        // hands the rest of this method to a thread-pool thread, so the web host
        // finishes starting and serves the report while the loop runs.
        await Task.Yield();

        using var consumer = _consumerFactory(_consumerConfig);

        consumer.Subscribe(JobCreatedPayload.Topic);

        _logger.LogInformation(
            "Subscribed to {Topic} as group {GroupId}.",
            JobCreatedPayload.Topic,
            JobCreatedPayload.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;

                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    // A broker or transport failure, not a message this loop was
                    // handed. There is nothing to commit and nothing to skip -
                    // the client reconnects on its own, so read again.
                    _logger.LogError(ex, "Consuming from {Topic} failed.", JobCreatedPayload.Topic);
                    continue;
                }

                // Two failures with opposite right answers, which is why they
                // are caught separately rather than under one catch.
                //
                //   A message that will not deserialize will not deserialize on
                //   the thousandth attempt either. Not committing it would stop
                //   the projection dead on that one message and every later
                //   event behind it, so its offset is committed and the loop
                //   moves past it. It is logged in full first, because
                //   committing past a message is the one thing here that
                //   discards data.
                //
                //   A write that failed, failed for a reason outside the
                //   message - the database was down, a connection dropped. The
                //   same message will succeed once that is fixed, so its offset
                //   is not committed and it is read again.
                JobCreatedPayload? payload;

                try
                {
                    payload = Deserialize(result.Message.Value);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "Discarding malformed message at {TopicPartitionOffset}. Value: {Value}",
                        result.TopicPartitionOffset,
                        result.Message.Value);

                    consumer.Commit(result);
                    continue;
                }

                if (payload is null)
                {
                    // Valid JSON, but not an event: a bare null, or an envelope
                    // with no payload. Malformed in the same sense - no retry
                    // will make a payload appear - so it is treated the same.
                    _logger.LogError(
                        "Discarding message with no payload at {TopicPartitionOffset}. Value: {Value}",
                        result.TopicPartitionOffset,
                        result.Message.Value);

                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    await UpsertAsync(payload);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Projecting job {JobId} from {TopicPartitionOffset} failed. It will be read again.",
                        payload.JobId,
                        result.TopicPartitionOffset);

                    // Not committing is not enough on its own. The consumer's
                    // in-memory position has already moved past this message, so
                    // the next Consume would return the one after it and this
                    // event would only come back around after a rebalance.
                    // Seeking to its offset is what makes the next read return
                    // this message again.
                    consumer.Seek(result.TopicPartitionOffset);

                    await Task.Delay(WriteRetryDelay, stoppingToken);
                    continue;
                }

                // Commit last. The row is written and the upsert is idempotent,
                // so if the process dies between here and the commit the event
                // is redelivered and overwrites the row it already wrote - which
                // changes nothing. That is the whole reason the write is allowed
                // to go first.
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown. Consume and Delay both throw this when the host stops,
            // and neither is an error.
            _logger.LogInformation("Stopping the {Topic} consumer.", JobCreatedPayload.Topic);
        }
        finally
        {
            // Leave the group deliberately rather than waiting to be timed out,
            // so the partitions are reassigned immediately instead of going
            // unread for the length of the session timeout.
            consumer.Close();
        }
    }

    // Internal rather than private so the contract tests can pin the mapping
    // from a real published message onto the types this service redefined -
    // the drift those redefinitions risk is the whole reason to test them.
    internal static JobCreatedPayload? Deserialize(string value)
    {
        var envelope = JsonSerializer.Deserialize<EventEnvelope<JobCreatedPayload>>(value, SerializerOptions);

        return envelope?.Payload;
    }

    // The repository is scoped and this class is a singleton, so it cannot be
    // taken in the constructor: a scoped dependency resolved once would outlive
    // every scope and hold what it captured for the life of the process. One
    // scope per message keeps it to the lifetime it was registered with.
    private async Task UpsertAsync(JobCreatedPayload payload)
    {
        using var scope = _scopeFactory.CreateScope();

        var repository = scope.ServiceProvider.GetRequiredService<IJobProjectionRepository>();

        await repository.UpsertAsync(new JobProjection
        {
            JobId = payload.JobId,
            JobReference = payload.JobReference,
            Status = payload.Status,
            Region = payload.Region,
            Priority = payload.Priority,
            ServiceCategory = payload.ServiceCategory,
            // The event's createdAt is when the job was raised upstream, which
            // is what job_created_at holds. When this row was written is
            // projected_at, and the database supplies that itself.
            JobCreatedAt = payload.CreatedAt
        });
    }
}

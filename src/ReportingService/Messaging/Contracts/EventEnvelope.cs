namespace ReportingService.Messaging.Contracts;

// The six-field envelope every ASSMS event carries, as fixed by
// docs/kafka/event-contracts.md in assms-platform-infrastructure.
//
// This is a redefinition of the same shape the Job Service publishes, not a
// reference to its type. There is no shared package: one would be a fourth
// thing to version and release for a student project with three services, and
// a service that has to take a package update before it can deploy is coupled
// in exactly the way events are meant to avoid. The cost of the choice is that
// this file and the Job Service's must be kept in step by hand - the document
// above, not either copy, is what they are both kept in step with.
//
// Deserialization is by name, so unknown fields are ignored and only what this
// service names here is read.
public class EventEnvelope<TPayload>
{
    /// <summary>Unique id of this event instance. A redelivery carries the same value.</summary>
    public string EventId { get; set; } = string.Empty;

    /// <summary>The event name in PascalCase, for example JobCreated.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>Schema version of Payload. Not a semantic version - an integer that starts at 1 per event type.</summary>
    public int EventVersion { get; set; }

    /// <summary>When the business fact happened in the producing service, not when the message was published.</summary>
    public DateTime OccurredAt { get; set; }

    /// <summary>Service that emitted the event, using the repository service name.</summary>
    public string Producer { get; set; } = string.Empty;

    /// <summary>The event-specific body.</summary>
    public TPayload Payload { get; set; } = default!;
}

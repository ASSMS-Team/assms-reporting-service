using System.Reflection;
using ReportingService.Controllers;
using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;
using ReportingService.Security;
using Microsoft.AspNetCore.Mvc;

namespace ReportingService.Tests;

// Report filter validation, JobStatusChanged event validity and default values
// that the existing tests did not pin.
public class ReportingMutationTests
{
    private const string JobId = "0ded1501-9e25-41bc-a6e3-cf8208bbd650";
    private const string InvalidTimestamp = "from must be an RFC 3339 UTC timestamp, for example 2026-09-15T10:30:00Z.";

    // ---------- Default values ----------

    private static readonly string[] Namespaces =
        ["ReportingService.DTOs", "ReportingService.Models", "ReportingService.Messaging.Contracts"];

    public static IEnumerable<object[]> Types() =>
        typeof(ReportsController).Assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false, IsPublic: true, IsGenericTypeDefinition: false }
                        && Namespaces.Contains(t.Namespace)
                        && t.GetConstructor(Type.EmptyTypes) is not null
                        && !t.GetCustomAttributes().Any(a => a.GetType().Name == "RequiredMemberAttribute"))
            .Select(t => new object[] { t.FullName! });

    [Theory]
    [MemberData(nameof(Types))]
    public void NonNullableStringProperties_DefaultToEmpty(string typeName) =>
        AssertStringDefaults(typeof(ReportsController).Assembly.GetType(typeName)!);

    [Fact]
    public void EventEnvelope_StringPropertiesDefaultToEmpty() =>
        AssertStringDefaults(typeof(EventEnvelope<JobStatusChangedPayload>));

    [Fact]
    public void JwtOptions_DefaultToTheSharedIssuerAndAudience_AndNoKey()
    {
        var options = new JwtOptions();

        Assert.Equal("assms-customer-asset-service", options.Issuer);
        Assert.Equal("assms-internal", options.Audience);
        Assert.Equal(string.Empty, options.SigningKey);
    }

    [Fact]
    public void ResponseCollections_DefaultToEmptyLists()
    {
        Assert.Empty(new DTOs.JobsByStatusResponse().Statuses);
        Assert.Empty(new DTOs.JobsByTechnicianResponse().Technicians);
        Assert.Empty(new DTOs.JobCompletionReportResponse().Jobs);
    }

    private static void AssertStringDefaults(Type type)
    {
        var instance = Activator.CreateInstance(type)!;
        var nullability = new NullabilityInfoContext();
        foreach (var property in type.GetProperties().Where(p => p.PropertyType == typeof(string) && p.CanRead))
        {
            var value = (string?)property.GetValue(instance);
            if (nullability.Create(property).ReadState == NullabilityState.NotNull)
                Assert.True(value == string.Empty, $"{type.Name}.{property.Name} should default to an empty string but was '{value}'.");
            else
                Assert.Null(value);
        }
    }

    // ---------- JobStatusChanged validity: every condition is required ----------

    private static EventEnvelope<JobStatusChangedPayload> ValidEnvelope() => new()
    {
        EventId = "7b41e9c6-0d38-4a52-9f17-3c8b6e2d5a04",
        EventType = JobStatusChangedPayload.EventType,
        EventVersion = JobStatusChangedPayload.EventVersion,
        Producer = JobStatusChangedPayload.Producer,
        OccurredAt = new DateTime(2026, 9, 22, 8, 0, 0, DateTimeKind.Utc),
        Payload = new JobStatusChangedPayload
        {
            JobId = JobId, JobReference = "JOB-7K2M9X", OldStatus = "IN_PROGRESS", NewStatus = "COMPLETED",
            OccurredAt = new DateTime(2026, 9, 22, 8, 0, 0, DateTimeKind.Utc)
        }
    };

    [Fact]
    public void IsUsable_AcceptsAFullyValidEvent()
    {
        Assert.True(JobStatusChangedConsumer.IsUsable(ValidEnvelope(), JobId));
    }

    public static IEnumerable<object[]> BrokenEvents() => new Action<EventEnvelope<JobStatusChangedPayload>>[]
    {
        e => e.Payload = null!,
        e => e.EventId = "not-a-guid",
        e => e.EventType = "JobCreated",
        e => e.EventVersion = 2,
        e => e.Producer = "dispatch-service",
        e => e.OccurredAt = default,
        e => e.OccurredAt = DateTime.SpecifyKind(e.OccurredAt, DateTimeKind.Unspecified),
        e => e.Payload.JobId = "not-a-guid",
        e => e.Payload.JobReference = " ",
        e => e.Payload.OldStatus = "",
        e => e.Payload.NewStatus = " ",
        e => e.Payload.OccurredAt = default,
        e => e.Payload.OccurredAt = DateTime.SpecifyKind(e.Payload.OccurredAt, DateTimeKind.Local),
    }.Select((breakIt, index) => new object[] { index, breakIt });

    [Theory]
    [MemberData(nameof(BrokenEvents))]
    public void IsUsable_RejectsAnEventWithAnyOneConditionBroken(int caseNumber, Action<EventEnvelope<JobStatusChangedPayload>> breakIt)
    {
        var envelope = ValidEnvelope();
        breakIt(envelope);

        Assert.False(JobStatusChangedConsumer.IsUsable(envelope, JobId), $"case {caseNumber} should be rejected");
    }

    [Fact]
    public void IsUsable_RejectsAMessageKeyThatIsNotTheJobId()
    {
        Assert.False(JobStatusChangedConsumer.IsUsable(ValidEnvelope(), "another-key"));
        Assert.False(JobStatusChangedConsumer.IsUsable(null, JobId));
    }

    // ---------- Report filters ----------

    private static void AssertErrors(IActionResult result, params (string Key, string Message)[] expected)
    {
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(bad.Value);
        Assert.Equal(400, problem.Status);
        Assert.Equal(expected.Length, problem.Errors.Count);
        foreach (var (key, message) in expected)
            Assert.Equal(message, Assert.Single(problem.Errors[key]));
    }

    public static IEnumerable<object[]> Regions() =>
        new[] { "WESTERN", "CENTRAL", "SOUTHERN", "NORTHERN", "EASTERN", "NORTH_WESTERN", "NORTH_CENTRAL", "UVA", " sabaragamuwa " }
            .Select(r => new object[] { r });

    [Theory]
    [MemberData(nameof(Regions))]
    public async Task TechnicianAndCompletionReports_AcceptEverySupportedRegion(string region)
    {
        var repository = new FakeJobProjectionRepository();
        var controller = new ReportsController(repository);

        Assert.IsType<OkObjectResult>(await controller.GetJobsByTechnician(null, null, region));
        Assert.Equal(region.Trim().ToUpperInvariant(), repository.TechnicianQueriedRegion);
        Assert.IsType<OkObjectResult>(await controller.GetJobCompletions(null, null, region));
    }

    [Fact]
    public async Task TechnicianAndCompletionReports_RejectAnUnsupportedRegion()
    {
        var controller = new ReportsController(new FakeJobProjectionRepository());

        AssertErrors(await controller.GetJobsByTechnician(null, null, "LONDON"), ("region", "region must be a supported normalized region value."));
        AssertErrors(await controller.GetJobCompletions(null, null, "LONDON"), ("region", "region must be a supported normalized region value."));
    }

    [Fact]
    public async Task TechnicianAndCompletionReports_ReportBothInvalidTimestamps()
    {
        var controller = new ReportsController(new FakeJobProjectionRepository());
        var toMessage = "to must be an RFC 3339 UTC timestamp, for example 2026-09-15T10:30:00Z.";

        AssertErrors(await controller.GetJobsByTechnician("yesterday", "today", null), ("from", InvalidTimestamp), ("to", toMessage));
        AssertErrors(await controller.GetJobCompletions("yesterday", "today", null), ("from", InvalidTimestamp), ("to", toMessage));
    }

    [Theory]
    [InlineData("2026-09-15T10:30:00Z", "2026-09-15T10:30:00Z")]
    [InlineData("2026-09-16T00:00:00Z", "2026-09-15T00:00:00Z")]
    public async Task TechnicianAndCompletionReports_RequireFromEarlierThanTo(string from, string to)
    {
        var controller = new ReportsController(new FakeJobProjectionRepository());

        AssertErrors(await controller.GetJobsByTechnician(from, to, null), ("from", "from must be earlier than to."));
        AssertErrors(await controller.GetJobCompletions(from, to, null), ("from", "from must be earlier than to."));
    }

    [Theory]
    [InlineData("2026-09-15T00:00:00Z", null)]
    [InlineData(null, "2026-09-15T00:00:00Z")]
    [InlineData("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z")]
    public async Task TechnicianAndCompletionReports_AcceptOpenAndOrderedRanges(string? from, string? to)
    {
        var controller = new ReportsController(new FakeJobProjectionRepository());

        Assert.IsType<OkObjectResult>(await controller.GetJobsByTechnician(from, to, null));
        Assert.IsType<OkObjectResult>(await controller.GetJobCompletions(from, to, null));
    }

    [Fact]
    public async Task JobsByStatus_AllowsAnEqualRange_ButRejectsFromAfterTo()
    {
        var controller = new ReportsController(new FakeJobProjectionRepository());

        Assert.IsType<OkObjectResult>(await controller.GetJobsByStatus("2026-09-15T10:30:00Z", "2026-09-15T10:30:00Z"));
        Assert.IsType<OkObjectResult>(await controller.GetJobsByStatus("2026-09-15T10:30:00Z", null));
        Assert.IsType<OkObjectResult>(await controller.GetJobsByStatus(null, "2026-09-15T10:30:00Z"));
        AssertErrors(await controller.GetJobsByStatus("2026-09-16T00:00:00Z", "2026-09-15T00:00:00Z"), ("from", "from must not be after to."));
    }
}

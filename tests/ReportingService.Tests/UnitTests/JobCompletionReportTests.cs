using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ReportingService.Controllers;
using ReportingService.DTOs;
using ReportingService.Models;

namespace ReportingService.Tests;

public class JobCompletionReportTests
{
    private static ReportsController Build(FakeJobProjectionRepository repository) => new(repository);

    private static JobCompletionReportResponse Ok(IActionResult result) =>
        Assert.IsType<JobCompletionReportResponse>(Assert.IsType<OkObjectResult>(result).Value);

    private static ValidationProblemDetails BadRequest(IActionResult result) =>
        Assert.IsType<ValidationProblemDetails>(Assert.IsType<BadRequestObjectResult>(result).Value);

    [Fact]
    public async Task GetJobCompletions_ReturnsCompletedJobsAndTotal()
    {
        var completedAt = new DateTime(2026, 9, 22, 14, 0, 0, DateTimeKind.Utc);
        var repository = new FakeJobProjectionRepository
        {
            JobCompletionsToReturn = new[]
            {
                new JobCompletionRecord
                {
                    JobId = "job-1",
                    JobReference = "JOB-001",
                    Region = "WESTERN",
                    ServiceCategory = "REPAIR",
                    TechnicianId = "tech-1",
                    TechnicianReference = "TEC-001",
                    CompletedAt = completedAt,
                },
                new JobCompletionRecord
                {
                    JobId = "job-2",
                    JobReference = "JOB-002",
                    Region = "CENTRAL",
                    ServiceCategory = "MAINTENANCE",
                    TechnicianId = "tech-2",
                    TechnicianReference = "TEC-002",
                    CompletedAt = completedAt,
                }
            }
        };

        var response = Ok(await Build(repository).GetJobCompletions(null, null, null));

        Assert.Equal(2, response.Total);
        Assert.Collection(response.Jobs,
            first =>
            {
                Assert.Equal("JOB-001", first.JobReference);
                Assert.Equal("WESTERN", first.Region);
                Assert.Equal("REPAIR", first.ServiceCategory);
                Assert.Equal("TEC-001", first.TechnicianReference);
                Assert.Equal(completedAt, first.CompletedAt);
            },
            second =>
            {
                Assert.Equal("JOB-002", second.JobReference);
                Assert.Equal("CENTRAL", second.Region);
                Assert.Equal("MAINTENANCE", second.ServiceCategory);
                Assert.Equal("TEC-002", second.TechnicianReference);
                Assert.Equal(completedAt, second.CompletedAt);
            });
    }

    [Fact]
    public async Task GetJobCompletions_WithNoMatchingCompletions_ReturnsEmpty200()
    {
        var repository = new FakeJobProjectionRepository();

        var response = Ok(await Build(repository).GetJobCompletions("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z", "WESTERN"));

        Assert.Empty(response.Jobs);
        Assert.Equal(0, response.Total);
        Assert.Equal(1, repository.GetJobCompletionsAsyncCallCount);
    }

    [Fact]
    public async Task GetJobCompletions_NormalizesRegionAndCombinesAllFilters()
    {
        var repository = new FakeJobProjectionRepository();

        var result = await Build(repository).GetJobCompletions(
            "2026-09-15T10:00:00Z", "2026-09-15T11:00:00Z", "western");

        Ok(result);

        Assert.Equal(new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc), repository.CompletionQueriedFrom);
        Assert.Equal(new DateTime(2026, 9, 15, 11, 0, 0, DateTimeKind.Utc), repository.CompletionQueriedTo);
        Assert.Equal("WESTERN", repository.CompletionQueriedRegion);
    }

    [Fact]
    public async Task GetJobCompletions_RejectsUnparseableFrom()
    {
        var result = BadRequest(await Build(new FakeJobProjectionRepository()).GetJobCompletions(
            "not-a-timestamp", null, null));

        Assert.Equal(StatusCodes.Status400BadRequest, result.Status);
        Assert.True(result.Errors.ContainsKey("from"));
    }

    [Fact]
    public async Task GetJobCompletions_RejectsUnparseableTo()
    {
        var result = BadRequest(await Build(new FakeJobProjectionRepository()).GetJobCompletions(
            null, "bad-timestamp", null));

        Assert.Equal(StatusCodes.Status400BadRequest, result.Status);
        Assert.True(result.Errors.ContainsKey("to"));
    }

    [Fact]
    public async Task GetJobCompletions_RejectsUnsupportedRegion()
    {
        var result = BadRequest(await Build(new FakeJobProjectionRepository()).GetJobCompletions(
            null, null, "ATLANTIS"));

        Assert.Equal(StatusCodes.Status400BadRequest, result.Status);
        Assert.True(result.Errors.ContainsKey("region"));
    }

    [Fact]
    public async Task GetJobCompletions_RejectsFromNotEarlierThanTo()
    {
        var result = BadRequest(await Build(new FakeJobProjectionRepository()).GetJobCompletions(
            "2026-09-15T12:00:00Z", "2026-09-15T11:00:00Z", null));

        Assert.Equal(StatusCodes.Status400BadRequest, result.Status);
        Assert.True(result.Errors.ContainsKey("from"));
    }
}

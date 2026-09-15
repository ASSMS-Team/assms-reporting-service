using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using ReportingService.Controllers;
using ReportingService.DTOs;
using ReportingService.Models;

namespace ReportingService.Tests;

public class JobsByTechnicianReportTests
{
    private static ReportsController Build(FakeJobProjectionRepository repository) => new(repository);

    private static JobsByTechnicianResponse Ok(IActionResult result) =>
        Assert.IsType<JobsByTechnicianResponse>(Assert.IsType<OkObjectResult>(result).Value);

    private static ValidationProblemDetails BadRequest(IActionResult result) =>
        Assert.IsType<ValidationProblemDetails>(Assert.IsType<BadRequestObjectResult>(result).Value);

    [Fact]
    public async Task GetJobsByTechnician_ReturnsGroupedCountsAndTotal()
    {
        var repository = new FakeJobProjectionRepository
        {
            TechnicianCountsToReturn = new[]
            {
                new TechnicianJobCount { TechnicianId = "tech-1", TechnicianReference = "TEC-001", JobCount = 2 },
                new TechnicianJobCount { TechnicianId = "tech-2", TechnicianReference = "TEC-002", JobCount = 1 },
            }
        };

        var response = Ok(await Build(repository).GetJobsByTechnician(null, null, null));

        Assert.Equal(3, response.Total);
        Assert.Collection(response.Technicians,
            first => { Assert.Equal("TEC-001", first.TechnicianReference); Assert.Equal(2, first.JobCount); },
            second => { Assert.Equal("TEC-002", second.TechnicianReference); Assert.Equal(1, second.JobCount); });
    }

    [Fact]
    public async Task GetJobsByTechnician_WithNoMatchingAssignments_ReturnsEmpty200()
    {
        var repository = new FakeJobProjectionRepository();

        var response = Ok(await Build(repository).GetJobsByTechnician("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z", "WESTERN"));

        Assert.Empty(response.Technicians);
        Assert.Equal(0, response.Total);
        Assert.Equal(1, repository.GetTechnicianJobCountsAsyncCallCount);
    }

    [Fact]
    public async Task GetJobsByTechnician_NormalizesRegionAndCombinesAllFilters()
    {
        var repository = new FakeJobProjectionRepository();

        var result = await Build(repository).GetJobsByTechnician(
            "2026-09-15T10:00:00Z", "2026-09-15T11:00:00Z", "western");

        Ok(result);
        Assert.Equal(new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc), repository.TechnicianQueriedFrom);
        Assert.Equal(new DateTime(2026, 9, 15, 11, 0, 0, DateTimeKind.Utc), repository.TechnicianQueriedTo);
        Assert.Equal("WESTERN", repository.TechnicianQueriedRegion);
    }

    [Fact]
    public async Task GetJobsByTechnician_AcceptsRfc3339OffsetsAndNormalizesThemToUtc()
    {
        var repository = new FakeJobProjectionRepository();

        var result = await Build(repository).GetJobsByTechnician("2026-09-15T15:30:00+05:30", null, null);

        Ok(result);
        Assert.Equal(new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc), repository.TechnicianQueriedFrom);
    }

    [Theory]
    [InlineData("2026-09-15")]
    [InlineData("last Tuesday")]
    public async Task GetJobsByTechnician_RejectsNonRfc3339Timestamp(string invalidFrom)
    {
        var repository = new FakeJobProjectionRepository();

        var problem = BadRequest(await Build(repository).GetJobsByTechnician(invalidFrom, null, null));

        Assert.True(problem.Errors.ContainsKey("from"));
        Assert.Equal(0, repository.GetTechnicianJobCountsAsyncCallCount);
    }

    [Fact]
    public async Task GetJobsByTechnician_RejectsUnknownRegion()
    {
        var repository = new FakeJobProjectionRepository();

        var problem = BadRequest(await Build(repository).GetJobsByTechnician(null, null, "COASTAL"));

        Assert.True(problem.Errors.ContainsKey("region"));
        Assert.Equal(0, repository.GetTechnicianJobCountsAsyncCallCount);
    }

    [Theory]
    [InlineData("2026-09-15T10:00:00Z", "2026-09-15T10:00:00Z")]
    [InlineData("2026-09-15T11:00:00Z", "2026-09-15T10:00:00Z")]
    public async Task GetJobsByTechnician_RequiresFromEarlierThanTo(string from, string to)
    {
        var repository = new FakeJobProjectionRepository();

        var problem = BadRequest(await Build(repository).GetJobsByTechnician(from, to, null));

        Assert.True(problem.Errors.ContainsKey("from"));
        Assert.Equal(StatusCodes.Status400BadRequest, problem.Status);
        Assert.Equal(0, repository.GetTechnicianJobCountsAsyncCallCount);
    }
}

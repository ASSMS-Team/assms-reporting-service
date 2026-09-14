using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using ReportingService.Controllers;
using ReportingService.DTOs;
using ReportingService.Models;

namespace ReportingService.Tests;

// The jobs-by-technician report - the endpoint the staging evidence flow calls to
// show that a JobAssigned event travelled all the way from Dispatch into a report
// a Manager can read.
public class JobsByTechnicianReportTests
{
    private static TechnicianJobCount Count(string reference, int jobCount, string? id = null) => new()
    {
        TechnicianId = id ?? $"id-for-{reference}",
        TechnicianReference = reference,
        JobCount = jobCount
    };

    private static ReportsController BuildController(FakeJobProjectionRepository repository) => new(repository);

    // Unwraps a 200 and its body in one step, so each test's assertions are about
    // the report rather than about ActionResult plumbing.
    private static JobsByTechnicianResponse AssertOk(IActionResult result)
    {
        var ok = Assert.IsType<OkObjectResult>(result);

        return Assert.IsType<JobsByTechnicianResponse>(ok.Value);
    }

    private static ValidationProblemDetails AssertBadRequest(IActionResult result)
    {
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);

        // Set explicitly on the body as well as on the result: a client reading
        // the problem document should not have to consult the HTTP status to
        // learn what it is.
        Assert.Equal(StatusCodes.Status400BadRequest, problem.Status);

        return problem;
    }

    // ---- the report itself -------------------------------------------------

    [Fact]
    public async Task GetJobsByTechnician_ReturnsEveryTechnicianTheRepositoryFound()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository
        {
            TechnicianCountsToReturn = new[] { Count("TECH-0001", 5), Count("TECH-0002", 2) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: null);

        // Assert - both the id and the human-readable reference travel out. The
        // reference is what a Manager reads; the id is what another service would
        // join on.
        var response = AssertOk(result);

        Assert.Collection(
            response.Technicians,
            first =>
            {
                Assert.Equal("TECH-0001", first.TechnicianReference);
                Assert.Equal("id-for-TECH-0001", first.TechnicianId);
                Assert.Equal(5, first.JobCount);
            },
            second =>
            {
                Assert.Equal("TECH-0002", second.TechnicianReference);
                Assert.Equal(2, second.JobCount);
            });
    }

    [Fact]
    public async Task GetJobsByTechnician_Total_SumsTheJobsNotTheTechnicians()
    {
        // Arrange - three technicians, twelve jobs. The two numbers differ on
        // purpose: a total that counted rows would return 3 here and look
        // plausible in every test where the counts happen to be one.
        var repository = new FakeJobProjectionRepository
        {
            TechnicianCountsToReturn = new[]
            {
                Count("TECH-0001", 7), Count("TECH-0002", 3), Count("TECH-0003", 2)
            }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        Assert.Equal(3, response.Technicians.Count);
        Assert.Equal(12, response.Total);
    }

    [Fact]
    public async Task GetJobsByTechnician_PreservesTheRepositoryOrdering()
    {
        // Arrange - the ordering is the repository's to decide, and the controller
        // must not re-sort. A report that reshuffles between refreshes is hard to
        // read, and the ordering is settled in SQL where it can be indexed.
        var repository = new FakeJobProjectionRepository
        {
            TechnicianCountsToReturn = new[] { Count("TECH-0009", 9), Count("TECH-0001", 4), Count("TECH-0005", 1) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        Assert.Equal(
            new[] { "TECH-0009", "TECH-0001", "TECH-0005" },
            response.Technicians.Select(technician => technician.TechnicianReference));
    }

    [Fact]
    public async Task GetJobsByTechnician_WithNoMatchingAssignments_ReturnsAnEmptyArrayAndNotA404()
    {
        // Arrange - the fake returns no counts by default, which is what an empty
        // projection or a range nothing falls into produces.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "2026-01-01", to: "2026-01-31");

        // Assert - 200, not 404. A 404 would say the report does not exist; what
        // happened is that the report ran and the answer was none, and a caller
        // drawing a chart has to be able to tell those apart.
        Assert.IsNotType<NotFoundResult>(result);
        Assert.IsNotType<NotFoundObjectResult>(result);

        var response = AssertOk(result);

        Assert.Empty(response.Technicians);
        Assert.Equal(0, response.Total);

        // The query really was run - an empty answer is an answer, not a shortcut
        // taken before reaching the database.
        Assert.Equal(1, repository.GetTechnicianJobCountsAsyncCallCount);
    }

    [Fact]
    public async Task GetJobsByTechnician_DoesNotInventAZeroRowForAnIdleTechnician()
    {
        // Arrange - a real and deliberate limit of this report: the service
        // projects assignments, not technicians, so it cannot know an idle
        // technician exists. Only Dispatch, which owns the roster, could answer
        // that. Pinned here so the absence reads as a decision rather than a bug.
        var repository = new FakeJobProjectionRepository
        {
            TechnicianCountsToReturn = new[] { Count("TECH-0001", 1) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        Assert.Equal("TECH-0001", Assert.Single(response.Technicians).TechnicianReference);
    }

    // ---- the filters -------------------------------------------------------

    [Fact]
    public async Task GetJobsByTechnician_WithNoFilters_QueriesWithNoBounds()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: null);

        // Assert - null bounds, not a range wide enough to match everything. The
        // repository omits the WHERE clause entirely on these.
        AssertOk(result);
        Assert.Equal(1, repository.GetTechnicianJobCountsAsyncCallCount);
        Assert.Null(repository.TechnicianQueriedFrom);
        Assert.Null(repository.TechnicianQueriedTo);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task GetJobsByTechnician_WithBlankFilters_TreatsThemAsAbsent(string blank)
    {
        // Arrange - ?from= is a caller who cleared the field, not a caller who
        // sent a broken date, so it must not be a 400.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: blank, to: blank);

        // Assert
        AssertOk(result);
        Assert.Null(repository.TechnicianQueriedFrom);
        Assert.Null(repository.TechnicianQueriedTo);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithADateOnlyBound_ReadsItAsMidnightUtc()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "2026-09-01", to: "2026-09-30");

        // Assert - Utc, not Unspecified and not Local. assigned_at is stored in
        // UTC, so a bound read as the server's local time would silently shift the
        // report by the timezone of whatever machine it ran on.
        AssertOk(result);

        Assert.Equal(new DateTime(2026, 9, 1, 0, 0, 0), repository.TechnicianQueriedFrom);
        Assert.Equal(DateTimeKind.Utc, repository.TechnicianQueriedFrom!.Value.Kind);

        Assert.Equal(new DateTime(2026, 9, 30, 0, 0, 0), repository.TechnicianQueriedTo);
        Assert.Equal(DateTimeKind.Utc, repository.TechnicianQueriedTo!.Value.Kind);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithAnOffsetBound_ConvertsItToUtc()
    {
        // Arrange - Sri Lanka is UTC+5:30, so a bound written with that offset has
        // to be converted rather than compared as written.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "2026-09-14T09:00:00+05:30", to: null);

        // Assert
        AssertOk(result);

        Assert.Equal(new DateTime(2026, 9, 14, 3, 30, 0), repository.TechnicianQueriedFrom);
        Assert.Equal(DateTimeKind.Utc, repository.TechnicianQueriedFrom!.Value.Kind);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithOnlyFrom_LeavesTheUpperBoundOpen()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "2026-09-01", to: null);

        // Assert
        AssertOk(result);
        Assert.NotNull(repository.TechnicianQueriedFrom);
        Assert.Null(repository.TechnicianQueriedTo);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithOnlyTo_LeavesTheLowerBoundOpen()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: null, to: "2026-09-30");

        // Assert
        AssertOk(result);
        Assert.Null(repository.TechnicianQueriedFrom);
        Assert.NotNull(repository.TechnicianQueriedTo);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithFromEqualToTo_IsAccepted()
    {
        // Arrange - the range is inclusive at both ends, so a single instant is a
        // legal range rather than a backwards one.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(
            from: "2026-09-14T09:00:00Z",
            to: "2026-09-14T09:00:00Z");

        // Assert
        AssertOk(result);
        Assert.Equal(1, repository.GetTechnicianJobCountsAsyncCallCount);
    }

    // ---- rejected filters --------------------------------------------------

    [Fact]
    public async Task GetJobsByTechnician_WithAnUnparseableBound_Returns400()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "last Tuesday", to: null);

        // Assert
        var problem = AssertBadRequest(result);

        Assert.NotEmpty(problem.Errors);
    }

    [Fact]
    public async Task GetJobsByTechnician_WithFromAfterTo_Returns400KeyedOnFrom()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByTechnician(from: "2026-09-30", to: "2026-09-01");

        // Assert - keyed on "from" rather than on both: the range is one fact, and
        // reporting it twice makes it look like two problems.
        var problem = AssertBadRequest(result);

        Assert.True(problem.Errors.ContainsKey("from"));
    }

    [Fact]
    public async Task GetJobsByTechnician_WithInvalidFilters_DoesNotQueryTheProjection()
    {
        // Arrange - a rejected request must not reach the database. Running the
        // query and then discarding it would put load on the projection for an
        // answer nobody receives.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        await controller.GetJobsByTechnician(from: "not a date", to: null);
        await controller.GetJobsByTechnician(from: "2026-09-30", to: "2026-09-01");

        // Assert
        Assert.Equal(0, repository.GetTechnicianJobCountsAsyncCallCount);
    }
}

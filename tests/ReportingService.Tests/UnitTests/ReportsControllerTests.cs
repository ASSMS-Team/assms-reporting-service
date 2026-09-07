using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using ReportingService.Controllers;
using ReportingService.DTOs;
using ReportingService.Models;

namespace ReportingService.Tests;

public class ReportsControllerTests
{
    private static JobStatusCount Count(string status, int count) => new()
    {
        Status = status,
        Count = count
    };

    private static ReportsController BuildController(FakeJobProjectionRepository repository) =>
        new(repository);

    // Unwraps a 200 and its body in one step, so each test's assertions are
    // about the report rather than about ActionResult plumbing.
    private static JobsByStatusResponse AssertOk(IActionResult result)
    {
        var ok = Assert.IsType<OkObjectResult>(result);

        return Assert.IsType<JobsByStatusResponse>(ok.Value);
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
    public async Task GetJobsByStatus_ReturnsEveryStatusTheRepositoryFound()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository
        {
            CountsToReturn = new[] { Count("ASSIGNED", 3), Count("CREATED", 12) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        Assert.Collection(
            response.Statuses,
            first =>
            {
                Assert.Equal("ASSIGNED", first.Status);
                Assert.Equal(3, first.Count);
            },
            second =>
            {
                Assert.Equal("CREATED", second.Status);
                Assert.Equal(12, second.Count);
            });
    }

    [Fact]
    public async Task GetJobsByStatus_Total_SumsTheJobsNotTheStatuses()
    {
        // Arrange - three statuses, twelve jobs. The two numbers differ on
        // purpose: a total that counted rows would return 3 here and look
        // plausible in every test where the counts happen to be one.
        var repository = new FakeJobProjectionRepository
        {
            CountsToReturn = new[] { Count("ASSIGNED", 2), Count("COMPLETED", 3), Count("CREATED", 7) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        Assert.Equal(3, response.Statuses.Count);
        Assert.Equal(12, response.Total);
    }

    [Fact]
    public async Task GetJobsByStatus_WithAStatusTheServiceDoesNotKnow_ReportsItAnyway()
    {
        // Arrange - the projection deliberately holds no opinion about what the
        // set of statuses is, so a value added upstream has to travel all the
        // way out of the API rather than being filtered against a local list.
        var repository = new FakeJobProjectionRepository
        {
            CountsToReturn = new[] { Count("AWAITING_PARTS", 4) }
        };
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: null, to: null);

        // Assert
        var response = AssertOk(result);

        var only = Assert.Single(response.Statuses);
        Assert.Equal("AWAITING_PARTS", only.Status);
        Assert.Equal(4, only.Count);
    }

    [Fact]
    public async Task GetJobsByStatus_WithNoMatchingJobs_ReturnsAnEmptyArrayAndNotA404()
    {
        // Arrange - the fake returns no counts by default, which is what an
        // empty projection or a range nothing falls into produces.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-01-01", to: "2026-01-31");

        // Assert - 200, not 404. A 404 would say the report does not exist; what
        // happened is that the report ran and the answer was none, and a caller
        // drawing a chart has to be able to tell those apart.
        Assert.IsNotType<NotFoundResult>(result);
        Assert.IsNotType<NotFoundObjectResult>(result);

        var response = AssertOk(result);

        Assert.Empty(response.Statuses);
        Assert.Equal(0, response.Total);

        // The query really was run - an empty answer is an answer, not a
        // shortcut taken before reaching the database.
        Assert.Equal(1, repository.GetStatusCountsAsyncCallCount);
    }

    // ---- the filters -------------------------------------------------------

    [Fact]
    public async Task GetJobsByStatus_WithNoFilters_QueriesWithNoBounds()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: null, to: null);

        // Assert - null bounds, not a range wide enough to match everything.
        // The repository omits the WHERE clause entirely on these.
        AssertOk(result);
        Assert.Equal(1, repository.GetStatusCountsAsyncCallCount);
        Assert.Null(repository.QueriedFrom);
        Assert.Null(repository.QueriedTo);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task GetJobsByStatus_WithBlankFilters_TreatsThemAsAbsent(string blank)
    {
        // Arrange - ?from= is a caller who cleared the field, not a caller who
        // sent a broken date, so it must not be a 400.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: blank, to: blank);

        // Assert
        AssertOk(result);
        Assert.Null(repository.QueriedFrom);
        Assert.Null(repository.QueriedTo);
    }

    [Fact]
    public async Task GetJobsByStatus_WithADateOnlyBound_ReadsItAsMidnightUtc()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-01", to: "2026-08-31");

        // Assert - Utc, not Unspecified and not Local. The projection's
        // timestamps are UTC, so a bound read as the server's local time would
        // silently shift the report by the timezone of whatever machine it ran
        // on.
        AssertOk(result);

        Assert.Equal(new DateTime(2026, 8, 1, 0, 0, 0), repository.QueriedFrom);
        Assert.Equal(DateTimeKind.Utc, repository.QueriedFrom!.Value.Kind);

        Assert.Equal(new DateTime(2026, 8, 31, 0, 0, 0), repository.QueriedTo);
        Assert.Equal(DateTimeKind.Utc, repository.QueriedTo!.Value.Kind);
    }

    [Fact]
    public async Task GetJobsByStatus_WithAnOffsetBound_ConvertsItToUtc()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act - Sri Lanka is UTC+5:30, so 09:00 local is 03:30 UTC.
        var result = await controller.GetJobsByStatus(from: "2026-08-01T09:00:00+05:30", to: null);

        // Assert - converted, not compared as written.
        AssertOk(result);

        Assert.Equal(new DateTime(2026, 8, 1, 3, 30, 0), repository.QueriedFrom);
        Assert.Equal(DateTimeKind.Utc, repository.QueriedFrom!.Value.Kind);
    }

    [Fact]
    public async Task GetJobsByStatus_WithOnlyFrom_LeavesTheUpperBoundOpen()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-01", to: null);

        // Assert - the two bounds are independent; supplying one must not
        // invent the other.
        AssertOk(result);
        Assert.NotNull(repository.QueriedFrom);
        Assert.Null(repository.QueriedTo);
    }

    [Fact]
    public async Task GetJobsByStatus_WithOnlyTo_LeavesTheLowerBoundOpen()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: null, to: "2026-08-31");

        // Assert
        AssertOk(result);
        Assert.Null(repository.QueriedFrom);
        Assert.NotNull(repository.QueriedTo);
    }

    [Fact]
    public async Task GetJobsByStatus_WithFromEqualToTo_IsAccepted()
    {
        // Arrange - the range is inclusive at both ends, so a single-instant
        // range is a legitimate request rather than a backwards one.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-01", to: "2026-08-01");

        // Assert
        AssertOk(result);
        Assert.Equal(repository.QueriedFrom, repository.QueriedTo);
    }

    // ---- criterion 4: invalid filters --------------------------------------

    [Theory]
    [InlineData("not-a-date")]
    [InlineData("2026-13-01")]
    [InlineData("2026-02-30")]
    [InlineData("01/08/2026 but with words")]
    [InlineData("last Tuesday")]
    public async Task GetJobsByStatus_WithAnUnparseableFrom_Returns400KeyedOnFrom(string value)
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: value, to: null);

        // Assert - keyed by query parameter name, so the frontend can render the
        // message against the field the caller has to fix.
        var problem = AssertBadRequest(result);

        Assert.True(problem.Errors.ContainsKey("from"));
        Assert.False(problem.Errors.ContainsKey("to"));
    }

    [Fact]
    public async Task GetJobsByStatus_WithAnUnparseableTo_Returns400KeyedOnTo()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-01", to: "not-a-date");

        // Assert
        var problem = AssertBadRequest(result);

        Assert.True(problem.Errors.ContainsKey("to"));
        Assert.False(problem.Errors.ContainsKey("from"));
    }

    [Fact]
    public async Task GetJobsByStatus_WithBothBoundsUnparseable_ReportsBoth()
    {
        // Arrange - both are parsed before either is judged, so a caller who got
        // both wrong is told about both rather than fixing one and being sent
        // straight back for the other.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "yesterday", to: "soon");

        // Assert
        var problem = AssertBadRequest(result);

        Assert.Equal(2, problem.Errors.Count);
        Assert.True(problem.Errors.ContainsKey("from"));
        Assert.True(problem.Errors.ContainsKey("to"));
    }

    [Fact]
    public async Task GetJobsByStatus_WithFromAfterTo_Returns400KeyedOnFrom()
    {
        // Arrange
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-31", to: "2026-08-01");

        // Assert - one key, not two. The backwards range is a single fact, and
        // reporting it against both fields would make it look like two separate
        // problems.
        var problem = AssertBadRequest(result);

        var error = Assert.Single(problem.Errors);
        Assert.Equal("from", error.Key);
        Assert.Contains("must not be after", Assert.Single(error.Value));
    }

    [Fact]
    public async Task GetJobsByStatus_WithABackwardsRangeAndAnUnparseableBound_ReportsOnlyTheParseFailure()
    {
        // Arrange - "to" is not a date at all, so there is no range to be
        // backwards. Complaining about the ordering on top of that would be
        // noise over the real error.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var result = await controller.GetJobsByStatus(from: "2026-08-31", to: "rubbish");

        // Assert
        var problem = AssertBadRequest(result);

        var error = Assert.Single(problem.Errors);
        Assert.Equal("to", error.Key);
        Assert.DoesNotContain("must not be after", Assert.Single(error.Value));
    }

    [Fact]
    public async Task GetJobsByStatus_WithInvalidFilters_DoesNotQueryTheProjection()
    {
        // Arrange - a request that was refused must not have reached the
        // database, whichever kind of refusal it was.
        var repository = new FakeJobProjectionRepository();
        var controller = BuildController(repository);

        // Act
        var unparseable = await controller.GetJobsByStatus(from: "not-a-date", to: null);
        var backwards = await controller.GetJobsByStatus(from: "2026-08-31", to: "2026-08-01");

        // Assert
        AssertBadRequest(unparseable);
        AssertBadRequest(backwards);

        Assert.Equal(0, repository.GetStatusCountsAsyncCallCount);
    }
}

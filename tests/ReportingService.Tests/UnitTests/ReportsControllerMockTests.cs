using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using ReportingService.Controllers;
using ReportingService.DTOs;
using ReportingService.Models;
using ReportingService.Repositories;
using Xunit;

namespace ReportingService.Tests;

public class ReportsControllerMockTests
{
    private readonly Mock<IJobProjectionRepository> _repositoryMock;
    private readonly ReportsController _controller;

    public ReportsControllerMockTests()
    {
        _repositoryMock = new Mock<IJobProjectionRepository>(MockBehavior.Strict);
        _controller = new ReportsController(_repositoryMock.Object);
    }

    #region GetJobsByStatus Tests

    [Fact]
    public async Task GetJobsByStatus_ValidDateRange_QueriesRepositoryAndReturnsOkWithMappedCounts()
    {
        // Arrange
        var fromStr = "2026-08-01";
        var toStr = "2026-08-31";
        var expectedFrom = DateTime.SpecifyKind(DateTime.Parse(fromStr), DateTimeKind.Utc);
        var expectedTo = DateTime.SpecifyKind(DateTime.Parse(toStr), DateTimeKind.Utc);

        var counts = new List<JobStatusCount>
        {
            new() { Status = "CREATED", Count = 10 },
            new() { Status = "ASSIGNED", Count = 5 },
            new() { Status = "COMPLETED", Count = 3 }
        };

        _repositoryMock
            .Setup(r => r.GetStatusCountsAsync(expectedFrom, expectedTo))
            .ReturnsAsync(counts);

        // Act
        var result = await _controller.GetJobsByStatus(fromStr, toStr);

        // Assert
        var okResult = Assert.IsType<OkObjectResult>(result);
        var response = Assert.IsType<JobsByStatusResponse>(okResult.Value);
        Assert.Equal(18, response.Total);
        Assert.Equal(3, response.Statuses.Count);
        Assert.Equal("CREATED", response.Statuses[0].Status);
        Assert.Equal(10, response.Statuses[0].Count);

        _repositoryMock.Verify(r => r.GetStatusCountsAsync(expectedFrom, expectedTo), Times.Once);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("2026-08-01", null)]
    [InlineData(null, "2026-08-31")]
    public async Task GetJobsByStatus_OpenEndedBounds_PassesParsedOrNullToRepository(string? from, string? to)
    {
        // Arrange
        _repositoryMock
            .Setup(r => r.GetStatusCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>()))
            .ReturnsAsync(new List<JobStatusCount>());

        // Act
        var result = await _controller.GetJobsByStatus(from, to);

        // Assert
        var okResult = Assert.IsType<OkObjectResult>(result);
        var response = Assert.IsType<JobsByStatusResponse>(okResult.Value);
        Assert.Equal(0, response.Total);
        Assert.Empty(response.Statuses);

        _repositoryMock.Verify(r => r.GetStatusCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>()), Times.Once);
    }

    [Theory]
    [InlineData("invalid-date", "2026-08-31", "from")]
    [InlineData("2026-08-01", "not-a-date", "to")]
    [InlineData("abc", "xyz", "from")]
    public async Task GetJobsByStatus_UnparseableDate_ReturnsBadRequestWithoutQueryingDb(
        string? from,
        string? to,
        string expectedErrorKey)
    {
        // Act
        var result = await _controller.GetJobsByStatus(from, to);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Equal(StatusCodes.Status400BadRequest, problem.Status);
        Assert.Contains(expectedErrorKey, problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetStatusCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>()), Times.Never);
    }

    [Fact]
    public async Task GetJobsByStatus_FromAfterTo_ReturnsBadRequestWithSpecificError()
    {
        // Act (from is later than to)
        var result = await _controller.GetJobsByStatus("2026-09-01", "2026-08-01");

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains("from", problem.Errors.Keys);
        Assert.Equal("from must not be after to.", problem.Errors["from"][0]);

        _repositoryMock.Verify(r => r.GetStatusCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>()), Times.Never);
    }

    #endregion

    #region GetJobsByTechnician Tests

    [Fact]
    public async Task GetJobsByTechnician_ValidParameters_NormalizesRegionAndReturnsOk()
    {
        // Arrange
        var fromStr = "2026-08-01T00:00:00Z";
        var toStr = "2026-08-31T23:59:59Z";
        var regionInput = "western";
        var fromUtc = DateTime.Parse(fromStr).ToUniversalTime();
        var toUtc = DateTime.Parse(toStr).ToUniversalTime();

        var counts = new List<TechnicianJobCount>
        {
            new() { TechnicianId = "tech-1", TechnicianReference = "TEC-001", JobCount = 8 },
            new() { TechnicianId = "tech-2", TechnicianReference = "TEC-002", JobCount = 12 }
        };

        _repositoryMock
            .Setup(r => r.GetTechnicianJobCountsAsync(fromUtc, toUtc, "WESTERN"))
            .ReturnsAsync(counts);

        // Act
        var result = await _controller.GetJobsByTechnician(fromStr, toStr, regionInput);

        // Assert
        var okResult = Assert.IsType<OkObjectResult>(result);
        var response = Assert.IsType<JobsByTechnicianResponse>(okResult.Value);
        Assert.Equal(20, response.Total);
        Assert.Equal(2, response.Technicians.Count);
        Assert.Equal("tech-1", response.Technicians[0].TechnicianId);

        _repositoryMock.Verify(r => r.GetTechnicianJobCountsAsync(fromUtc, toUtc, "WESTERN"), Times.Once);
    }

    [Theory]
    [InlineData("2026-08-01", "2026-08-31T00:00:00Z", "from")] // missing time component / 'T'
    [InlineData("2026-08-01T00:00:00Z", "invalid", "to")]
    [InlineData("bad", "worse", "from")]
    public async Task GetJobsByTechnician_MalformedTimestamp_ReturnsBadRequest(string? from, string? to, string expectedErrorKey)
    {
        // Act
        var result = await _controller.GetJobsByTechnician(from, to, "WESTERN");

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains(expectedErrorKey, problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetTechnicianJobCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    [Theory]
    [InlineData("INVALID_REGION")]
    [InlineData("NORTH_EAST")]
    [InlineData("COLOMBO")]
    public async Task GetJobsByTechnician_UnsupportedRegion_ReturnsBadRequest(string invalidRegion)
    {
        // Act
        var result = await _controller.GetJobsByTechnician(null, null, invalidRegion);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains("region", problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetTechnicianJobCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    [Fact]
    public async Task GetJobsByTechnician_FromSameOrAfterTo_ReturnsBadRequest()
    {
        // Act
        var result = await _controller.GetJobsByTechnician("2026-08-10T12:00:00Z", "2026-08-10T12:00:00Z", null);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains("from", problem.Errors.Keys);
        Assert.Equal("from must be earlier than to.", problem.Errors["from"][0]);

        _repositoryMock.Verify(r => r.GetTechnicianJobCountsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    #endregion

    #region GetJobCompletions Tests

    [Fact]
    public async Task GetJobCompletions_ValidParameters_ReturnsOkWithCompletedJobs()
    {
        // Arrange
        var fromStr = "2026-08-01T00:00:00Z";
        var toStr = "2026-08-31T23:59:59Z";
        var fromUtc = DateTime.Parse(fromStr).ToUniversalTime();
        var toUtc = DateTime.Parse(toStr).ToUniversalTime();

        var records = new List<JobCompletionRecord>
        {
            new()
            {
                JobId = "job-1",
                JobReference = "JOB-001",
                Region = "WESTERN",
                ServiceCategory = "REPAIR",
                TechnicianId = "tech-1",
                TechnicianReference = "TEC-001",
                CompletedAt = DateTime.UtcNow
            }
        };

        _repositoryMock
            .Setup(r => r.GetJobCompletionsAsync(fromUtc, toUtc, "WESTERN"))
            .ReturnsAsync(records);

        // Act
        var result = await _controller.GetJobCompletions(fromStr, toStr, "western");

        // Assert
        var okResult = Assert.IsType<OkObjectResult>(result);
        var response = Assert.IsType<JobCompletionReportResponse>(okResult.Value);
        Assert.Equal(1, response.Total);
        Assert.Single(response.Jobs);
        Assert.Equal("job-1", response.Jobs[0].JobId);

        _repositoryMock.Verify(r => r.GetJobCompletionsAsync(fromUtc, toUtc, "WESTERN"), Times.Once);
    }

    [Theory]
    [InlineData("2026-08-01", "2026-08-31T00:00:00Z", "from")]
    [InlineData("2026-08-01T00:00:00Z", "bad-date", "to")]
    public async Task GetJobCompletions_MalformedTimestamp_ReturnsBadRequest(string? from, string? to, string expectedKey)
    {
        // Act
        var result = await _controller.GetJobCompletions(from, to, null);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains(expectedKey, problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetJobCompletionsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    [Theory]
    [InlineData("UNSUPPORTED")]
    [InlineData("SOUTH_WEST")]
    public async Task GetJobCompletions_UnsupportedRegion_ReturnsBadRequest(string invalidRegion)
    {
        // Act
        var result = await _controller.GetJobCompletions(null, null, invalidRegion);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains("region", problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetJobCompletionsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    [Fact]
    public async Task GetJobCompletions_FromAfterTo_ReturnsBadRequest()
    {
        // Act
        var result = await _controller.GetJobCompletions("2026-09-01T00:00:00Z", "2026-08-01T00:00:00Z", null);

        // Assert
        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var problem = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Contains("from", problem.Errors.Keys);

        _repositoryMock.Verify(r => r.GetJobCompletionsAsync(It.IsAny<DateTime?>(), It.IsAny<DateTime?>(), It.IsAny<string?>()), Times.Never);
    }

    #endregion
}

using System.Globalization;

using Microsoft.AspNetCore.Mvc;

using ReportingService.DTOs;
using ReportingService.Repositories;

namespace ReportingService.Controllers;

[ApiController]
[Route("api/reports")]
[Produces("application/json")]
public class ReportsController : ControllerBase
{
    // Both bounds arrive as strings rather than as DateTime?, so that an
    // unparseable value is this code's to report. Bound as DateTime? the
    // framework would reject it first, with its own wording and its own idea of
    // what a date looks like; parsed here, a bad value gets the same message
    // every time and both bounds are read as UTC, which is what the timestamps
    // in the projection are.
    private const string InvalidDateMessage =
        "Must be a date or an ISO 8601 date and time, for example 2026-08-01 or 2026-08-01T09:00:00Z.";

    private readonly IJobProjectionRepository _repository;

    public ReportsController(IJobProjectionRepository repository)
    {
        _repository = repository;
    }

    /// <summary>
    /// Counts projected jobs by lifecycle status, optionally within a date
    /// range. The jobs counted are those the Reporting read model has absorbed
    /// from the Job Service; the range is applied to when each job was created,
    /// not to when this service received it.
    /// </summary>
    /// <param name="from">Optional inclusive start of the range, as a date or an ISO 8601 date and time. A date with no time is read as midnight UTC. Omit for no lower bound.</param>
    /// <param name="to">Optional inclusive end of the range, in the same formats. Omit for no upper bound.</param>
    /// <response code="200">The counts. An empty array with a total of 0 when no job matches - that is an answer, not a missing resource.</response>
    /// <response code="400">A filter could not be used: a value that is not a date, or a from that falls after its to. Errors are keyed by query parameter name.</response>
    [HttpGet("jobs-by-status")]
    [ProducesResponseType(typeof(JobsByStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetJobsByStatus(
        [FromQuery] string? from,
        [FromQuery] string? to)
    {
        var errors = new Dictionary<string, string[]>();

        // Both are parsed before either is judged, so a request with two bad
        // values is told about both rather than about the first one twice.
        if (!TryParseBound(from, out var fromUtc))
        {
            errors["from"] = new[] { InvalidDateMessage };
        }

        if (!TryParseBound(to, out var toUtc))
        {
            errors["to"] = new[] { InvalidDateMessage };
        }

        // Only worth checking once both parsed - an ordering complaint about a
        // value that is not a date at all would be noise on top of the real
        // error.
        if (errors.Count == 0 && fromUtc.HasValue && toUtc.HasValue && fromUtc > toUtc)
        {
            // Keyed on "from" rather than on both: the range is one fact and
            // reporting it twice makes it look like two problems. from is the
            // field named first, so it is the one the message points at.
            errors["from"] = new[] { "from must not be after to." };
        }

        if (errors.Count > 0)
        {
            return BadRequest(new ValidationProblemDetails(errors)
            {
                Status = StatusCodes.Status400BadRequest
            });
        }

        var counts = await _repository.GetStatusCountsAsync(fromUtc, toUtc);

        // No rows is a 200 with an empty array. A 404 would say the report does
        // not exist, when what happened is that the report ran and the answer
        // was none - and a caller drawing a chart needs to tell "nothing
        // matched" apart from "this endpoint is wrong".
        return Ok(new JobsByStatusResponse
        {
            Statuses = counts
                .Select(count => new JobStatusCountResponse
                {
                    Status = count.Status,
                    Count = count.Count
                })
                .ToList(),
            Total = counts.Sum(count => count.Count)
        });
    }

    // Absent and blank are the same thing: ?from= is a caller who cleared the
    // field, not a caller who sent a broken date.
    //
    // AssumeUniversal with AdjustToUniversal is what makes the bound comparable
    // to job_created_at. A value with no offset is taken as UTC rather than as
    // the server's local time, so the report does not shift by the timezone of
    // whatever machine it happens to run on, and a value that does carry an
    // offset is converted rather than compared as written.
    private static bool TryParseBound(string? value, out DateTime? parsed)
    {
        parsed = null;

        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        if (!DateTime.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var result))
        {
            return false;
        }

        parsed = result;

        return true;
    }
}

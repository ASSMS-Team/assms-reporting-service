using ReportingService.Models;
using ReportingService.Repositories;

namespace ReportingService.Tests;

// Hand-rolled stand-in for the real repository: every call records what it was
// given and hands back whatever the test configured, so the controller and the
// consumer can be exercised without a database.
public class FakeJobProjectionRepository : IJobProjectionRepository
{
    // UpsertAsync - every projection written, in order. The consumer builds a
    // fresh object per message, so the list is a faithful record of what would
    // have reached the table.
    public readonly List<JobProjection> Upserted = new();
    public int UpsertAsyncCallCount;
    public readonly List<JobAssignmentProjection> Assignments = new();

    // Left null for the happy path. Set it to stand in for the database being
    // unreachable, which is the failure the consumer must retry rather than
    // commit past.
    public Exception? UpsertExceptionToThrow;

    // How many opening calls throw before one is allowed to succeed. Zero, the
    // default, never fails. Anything higher stands in for a database that was
    // down and came back, which is the case the retry exists for.
    public int UpsertFailuresBeforeSuccess;

    // Runs after the call is recorded and before the throw. The consumer tests
    // use it to cancel the host token at the moment of failure, so the loop
    // unwinds instead of sleeping out its retry delay.
    public Action? OnUpsert;

    // GetStatusCountsAsync - defaults to empty, which is the "no data" case the
    // report has to answer with a 200 rather than a 404.
    public IReadOnlyList<JobStatusCount> CountsToReturn = Array.Empty<JobStatusCount>();
    public int GetStatusCountsAsyncCallCount;
    public DateTime? QueriedFrom;
    public DateTime? QueriedTo;

    public Task UpsertAsync(JobProjection projection)
    {
        // Recorded and counted before the throw, so a test that configures a
        // failure can still assert on what the write was given.
        Upserted.Add(projection);
        UpsertAsyncCallCount++;

        OnUpsert?.Invoke();

        if (UpsertAsyncCallCount <= UpsertFailuresBeforeSuccess)
        {
            throw UpsertExceptionToThrow ?? new InvalidOperationException("The database was unreachable.");
        }

        if (UpsertExceptionToThrow is not null)
        {
            throw UpsertExceptionToThrow;
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<JobStatusCount>> GetStatusCountsAsync(DateTime? from, DateTime? to)
    {
        // Both bounds are recorded even when null: "the report was run with no
        // lower bound" is a distinct outcome from "the report was not run", and
        // the call count is what tells them apart.
        QueriedFrom = from;
        QueriedTo = to;
        GetStatusCountsAsyncCallCount++;

        return Task.FromResult(CountsToReturn);
    }

    // ApplyAssignmentAsync - what the JobAssigned consumer calls.
    public int ApplyAssignmentAsyncCallCount;

    // Left null for the happy path. Set it to stand in for the database being
    // unreachable, which is the failure the consumer must retry rather than
    // commit past.
    public Exception? ApplyAssignmentExceptionToThrow;

    // How many opening calls throw before one is allowed to succeed. Zero, the
    // default, never fails. Anything higher stands in for a database that was
    // down and came back, which is the case the seek-and-retry exists for.
    public int ApplyAssignmentFailuresBeforeSuccess;

    // Runs after the call is recorded and before the throw. The consumer tests
    // use it to cancel the host token at the moment of failure, so the loop
    // unwinds instead of sleeping out its retry delay.
    public Action? OnApplyAssignment;

    public Task ApplyAssignmentAsync(JobAssignmentProjection assignment)
    {
        // Recorded and counted before the throw, so a test that configures a
        // failure can still assert on what the write was given.
        Assignments.Add(assignment);
        ApplyAssignmentAsyncCallCount++;

        OnApplyAssignment?.Invoke();

        if (ApplyAssignmentAsyncCallCount <= ApplyAssignmentFailuresBeforeSuccess)
        {
            throw ApplyAssignmentExceptionToThrow
                ?? new InvalidOperationException("The database was unreachable.");
        }

        if (ApplyAssignmentExceptionToThrow is not null)
        {
            throw ApplyAssignmentExceptionToThrow;
        }

        return Task.CompletedTask;
    }

    // GetTechnicianJobCountsAsync - defaults to empty, which is the "no data"
    // case the report has to answer with a 200 rather than a 404.
    public IReadOnlyList<TechnicianJobCount> TechnicianCountsToReturn = Array.Empty<TechnicianJobCount>();
    public int GetTechnicianJobCountsAsyncCallCount;
    public DateTime? TechnicianQueriedFrom;
    public DateTime? TechnicianQueriedTo;

    public Task<IReadOnlyList<TechnicianJobCount>> GetTechnicianJobCountsAsync(DateTime? from, DateTime? to)
    {
        // Both bounds are recorded even when null: "the report was run with no
        // lower bound" is a distinct outcome from "the report was not run", and
        // the call count is what tells them apart.
        TechnicianQueriedFrom = from;
        TechnicianQueriedTo = to;
        GetTechnicianJobCountsAsyncCallCount++;

        return Task.FromResult(TechnicianCountsToReturn);
    }
}

# The `job_projection` read model

**Service:** Reporting Service · **Database:** `reportingdb` · **Table:** `job_projection`
**Source topic:** `job-created` · **Consumer group:** `assms-reporting-job-created`
**Event contract:** ASSMS Kafka Event Contracts · `assms-platform-infrastructure/docs/kafka/event-contracts.md`
**Report:** `GET /api/reports/jobs-by-status`

This is the first ASSMS service to **consume** an event rather than publish one, and the
first table in the system that holds a copy of data another service owns. Two decisions
below are deliberate departures from how the other services are built, and both are here
because a read model is not the same kind of table as a system of record.

---

## 1. The shape

```
Job Service ──→ Kafka `job-created` ──→ JobCreatedConsumer ──→ job_projection
                                                                     │
                                    GET /api/reports/jobs-by-status ──┘
```

| Column | Type | Notes |
|---|---|---|
| `job_id` | CHAR(36) | PK — the Job Service's id, reused. See section 2 |
| `job_reference` | VARCHAR(20) | The `JOB-` handle an Agent quotes |
| `status` | VARCHAR(20) | **No CHECK** — see section 3 |
| `region` | VARCHAR(50) | **No CHECK** — same reason |
| `priority` | VARCHAR(20) | |
| `service_category` | VARCHAR(50) | |
| `job_created_at` | TIMESTAMP | When the job was raised **upstream** |
| `projected_at` | TIMESTAMP | When this read model absorbed it. DB-written, refreshed on every upsert |

Indexes: PK on `job_id`, plain indexes on `status` and `job_created_at`. Neither is
speculative — the report groups by the first and filters by the second, so they are its
`GROUP BY` and its `WHERE`.

The two timestamps are not the same fact and the report must not confuse them.
`job_created_at` is a business time that came off the event; `projected_at` is a plumbing
time this service writes. The date filters run on the first, so a report of "jobs raised
in August" does not change because the consumer was down for a day and caught up in
September.

---

## 2. Why `job_id` is the primary key

Kafka delivers **at least once**. A rebalance, a redelivery, or a restart between the
write and the offset commit all re-present an event this service has already handled.

Making the upstream `job_id` the primary key is what makes that harmless. The consumer
writes with `INSERT ... ON DUPLICATE KEY UPDATE`, so the second arrival of a `JobCreated`
event collides with the key and **updates the row instead of inserting a second copy of
the same job**. There is no "have I seen this?" query, no `eventId` ledger, and no
exception to catch on a repeat — the engine settles it inside one statement.

A local surrogate key would have given every redelivery its own row and **doubled every
count in the report**, which is the one defect a counting report cannot survive.

That property is also what lets the consumer write before it commits. See section 4.

---

## 3. Why there are no CHECK constraints on `status` or `region`

**This is a deliberate difference from `jobs` in `jobdb`, which constrains `region`,
`priority` and `service_category`.**

`jobs` is where those values originate, so it is the right place to reject a bad one. By
the time a value reaches `job_projection` the Job Service has already accepted it, stored
it, and published it to every consumer. A CHECK here could only ever reject a value the
owning service considers valid.

The failure that would cause is worth being concrete about. Suppose a later sprint adds
`ASSIGNED` to the job lifecycle. The first such event arrives on a topic this consumer is
subscribed to, and with a CHECK:

1. the insert fails,
2. the write is treated as retryable — because from the consumer's point of view it is,
3. the offset is never committed,
4. the consumer re-reads that message forever and **stops making progress on every later
   event behind it**.

One new enum value upstream halts the whole report, and the two repositories have to be
released together to unblock it. Without the CHECK the row simply lands, the report grows
a row for the new status, and nothing needed coordinating.

Absorbing an unrecognised value is the correct behaviour for a projection. Rejecting it is
not. The column widths follow the same reasoning: `region` and `service_category` are
`VARCHAR(50)` here against the source's `VARCHAR(20)`, so a longer value upstream is
stored rather than truncated.

There are also **no foreign keys**, for the reason the `jobs` table has none: these columns
name rows in `jobdb`, a different database owned by a different service, and MySQL cannot
constrain across that boundary.

---

## 4. The contract types are redefined, not shared

`Messaging/Contracts/EventEnvelope.cs` and `Messaging/Contracts/JobCreatedPayload.cs`
restate the shape the Job Service publishes. They are **not** a reference to the Job
Service's types, and there is no shared NuGet package.

| | Shared package | Redefine per service |
|---|---|---|
| Duplication | none | the envelope exists in two repositories |
| Coupling | a service cannot deploy until it takes the package update | a service deploys on its own |
| Overhead | a fourth artefact to version, build and release | none |

For three services in a student project the package is overkill, and the coupling it adds
is the kind events exist to avoid. So the duplication is accepted — with the condition
that **the contract document, not either copy, is what both copies are kept in step
with.** A change to `docs/kafka/event-contracts.md` in `assms-platform-infrastructure`
is a change to both files.

The payload here declares only the **seven fields the projection stores**. The contract
also defines `customerId`, `assetId` and `problemDescription`; they are on the wire and
are deliberately not named, because a count of jobs by status has no use for them and a
consumer that names a field then has to keep up with it. Unknown JSON properties are
ignored on deserialization, so leaving them out costs nothing at read time — and it is
what the contract asks of a consumer, which must ignore fields it does not recognise.

> **Consumer group.** `assms-reporting-job-created`, exactly as the contract document's
> table names it, following its `assms-<consuming-service>-<topic>` convention. The rule
> that convention protects is that Dispatch and Reporting must **never share a group** on
> `job-created`: a shared group would make Kafka hand each event to only one of the two,
> and both need every event independently. Encoding the consuming service and the topic in
> the name is what makes an accidental collision hard. It is a constant on
> `JobCreatedPayload` rather than a configuration value — the group identifies this
> consumer, and it must be the same in every environment or Kafka would treat staging and
> production as separate readers of the same topic and replay from the start.

---

## 5. Deserialization failure and write failure are not the same failure

The consume loop is `consume → deserialize → upsert → commit`, and the two failure
paths in the middle are caught separately because they have **opposite** right answers.

| | Malformed message | Failed write |
|---|---|---|
| What happened | The bytes are not a `JobCreated` event | The database was down, a connection dropped |
| Will retrying help? | **No** — it will not parse on the thousandth attempt either | **Yes** — the same message succeeds once the database is back |
| Offset | **Committed**, so the loop moves past it | **Not committed** |
| Then | logged in full first, because this is the one path that discards data | `Seek` back to the offset, wait, read it again |

Folding them together forces a choice that is wrong one way or the other. Commit on every
failure and a database blip silently drops jobs from the report. Retry on every failure and
one malformed message — a hand-typed smoke test on the topic, say — blocks the projection
and every event queued behind it, permanently.

Two details that are easy to get wrong:

- **Not committing is not enough to cause a retry.** The consumer's in-memory position has
  already moved past the message, so the next `Consume` returns the *next* one and the
  failed event would only come back after a rebalance. `consumer.Seek(...)` back to its
  offset is what actually re-reads it.
- **The retry waits.** Without a delay, a database that is down turns the retry into a hot
  loop that reopens a connection thousands of times a second.

`EnableAutoCommit = false` is what makes any of this possible. Auto-commit moves the
offset on a timer with no idea whether the write succeeded, so a crash between the timer
firing and the write would lose that job from the report permanently.

### Why the commit comes after the write

The alternative — commit first, then write — loses an event whenever the process dies in
between, and a report quietly missing rows is worse than one that is late.

Writing first risks the opposite: the process dies after the write and before the commit,
so the event is redelivered and written a second time. That is exactly the case section 2
made harmless. **The idempotent upsert is what buys the right ordering here**; without it,
this ordering would double-count.

---

## 6. The report

`GET /api/reports/jobs-by-status?from=&to=`

```sql
SELECT status, COUNT(*) AS job_count
FROM job_projection
WHERE job_created_at >= @from AND job_created_at <= @to   -- each half only when supplied
GROUP BY status
ORDER BY status;
```

The two bounds are appended independently rather than as a single `BETWEEN`, because
either may be absent on its own. With both supplied the pair is exactly
`BETWEEN @from AND @to`, inclusive at each end; with neither, the `WHERE` clause is omitted
entirely rather than filled with a range wide enough to match everything.

`ORDER BY status` so the report reads the same way twice — without it MySQL may return the
groups in any order, and a table that reshuffles between refreshes is hard to read.

Statuses come out of the data, not out of a list this service keeps. A status no job holds
is **absent**, not reported as zero — which is the same reason section 3 gives for the
missing CHECK: this service does not hold an opinion about what the set of statuses is.

### Response

```json
{
  "statuses": [
    { "status": "CREATED", "count": 12 }
  ],
  "total": 12
}
```

An object wrapping the array rather than the bare array, because the total has to travel
with it: a caller cannot derive the total from the rows without assuming the rows are
complete, and once a later story pages or caps them that assumption stops holding.
`total` is the number of **jobs** counted, not the number of statuses.

### Invalid filters, and the empty case

| Request | Response |
|---|---|
| `from` or `to` is not a date | **400**, keyed on the offending parameter |
| both are unparseable | **400** naming **both** — they are parsed before either is judged |
| `from` is after `to` | **400**, keyed on `from` |
| valid filters, nothing matches | **200**, `{"statuses": [], "total": 0}` |

**Nothing matching is not a 404.** A 404 says the report does not exist. What happened is
that the report ran and the answer was none — a caller drawing a chart needs to tell
"nothing matched" apart from "this endpoint is wrong", and only the 200 says the first.

The ordering complaint is only raised once both values parsed. Telling a caller their range
is backwards when one end is not a date at all is noise on top of the real error.

Both parameters are bound as **strings and parsed here**, not as `DateTime?`. Bound as
`DateTime?` the framework would reject a bad value first, with its own wording and its own
idea of what a date looks like. Parsing here gives one consistent message and, more
importantly, control over the timezone: `AssumeUniversal | AdjustToUniversal` means a value
with no offset is read as **UTC** rather than as the server's local time, so the report does
not shift by the timezone of whatever machine it runs on. A value that does carry an offset
is converted rather than compared as written. `?from=` with an empty value is a caller who
cleared the field, and is treated as absent rather than as a broken date.

---

## 7. Files

| Layer | Files |
|---|---|
| Database | `database/migrations/V01__create_job_projection.sql` |
| Migration runner | `scripts/development/apply_migrations.ps1` (copied unchanged from the Job Service) |
| Contracts | `Messaging/Contracts/EventEnvelope.cs`, `Messaging/Contracts/JobCreatedPayload.cs` |
| Consumer | `Messaging/Consumers/JobCreatedConsumer.cs` |
| Models | `Models/JobProjection.cs`, `Models/JobStatusCount.cs` |
| Data access | `Repositories/IJobProjectionRepository.cs`, `Repositories/JobProjectionRepository.cs` |
| DTOs | `DTOs/JobsByStatusResponse.cs`, `DTOs/JobStatusCountResponse.cs` |
| HTTP | `Controllers/ReportsController.cs` |
| Wiring | `Program.cs`, `appsettings.Development.json`, `appsettings.Example.json` |
| Tests | `tests/ReportingService.Tests/UnitTests/*`, `tests/ReportingService.Tests/Fakes/*` |

`IDbConnectionFactory` and `MySqlConnectionFactory` already existed and were reused
unchanged.

### What the tests cover, and what they cannot

47 unit tests over three files, with hand-rolled fakes and no mocking library, matching the
Job Service's test style.

| File | What it pins |
|---|---|
| `ReportsControllerTests` | Section 6 in full — the total, the empty case as a 200, UTC bound handling, and every criterion-4 refusal |
| `JobCreatedContractTests` | Section 4 — the redefined types read the contract document's own example message, ignore undeclared fields, and tolerate a field added upstream |
| `JobCreatedConsumerTests` | Section 5 — commit past a malformed message, seek back on a failed write, commit only after the write, and the consumer config itself |

`JobCreatedConsumer` takes an **internal** constructor overload accepting a
`Func<ConsumerConfig, IConsumer<string, string>>`, and `Deserialize` is **internal** rather
than private; `ReportingService.csproj` grants the test project `InternalsVisibleTo`. The
public constructor is unchanged and still builds a real consumer. Without that seam the
commit-versus-seek branch — the single decision in this service most worth verifying —
would be reachable only with a running broker.

**`JobProjectionRepository` has no unit tests.** `IDbConnectionFactory` hands back a
concrete `MySqlConnection`, so there is nothing to fake that would still exercise the SQL,
and a test against a fake connection would verify a string rather than a query. The upsert
and the grouping query were instead checked by hand against the local MySQL 8.0.40
container: two events for one `job_id` left one row, an unconstrained status was absorbed,
and the grouped query returned what was expected. A proper integration test against a
disposable database is the right home for that, and is not written.

Applying the migration locally:

```powershell
.\scripts\development\apply_migrations.ps1 -Database reportingdb -User reporting_svc -Password 'ReportingLocalDev!23'
```

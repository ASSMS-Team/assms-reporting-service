# assms-reporting-service

## Overview

The Reporting Service is the **read side** of ASSMS. It owns no business data and accepts
no writes from anyone: it subscribes to the events the other services publish, keeps a
local read model of what it hears, and serves reports from that.

```
Job Service ──→ Kafka `job-created` ──→ JobCreatedConsumer ──→ reportingdb.job_projection
                                                                          │
                                            GET /api/reports/jobs-by-status
```

This is the first ASSMS service that **consumes** an event rather than publishing one, and
the first to hold a copy of data another service owns. Both make it different in kind from
the Customer & Asset and Job services, and the differences are deliberate — see
[Database Ownership](#database-ownership) and
[docs/read-model/job-projection.md](docs/read-model/job-projection.md).

## Responsibilities

**Does:**

- Consume `job-created` and keep `job_projection` current, idempotently.
- Serve `GET /api/reports/jobs-by-status`, with optional date bounds.
- Report its own liveness and database reachability.

**Does not:**

- Create, update or validate a job. Those belong to the Job Service, which owns `jobdb`.
- Act as a source of truth for anything. Every row here is a copy; the authority is the
  service that published it.
- Publish events. It is a consumer only.

## Technology

| | |
|---|---|
| Runtime | .NET 8 — SDK pinned to `8.0.424` in `global.json` |
| API | ASP.NET Core Web API, controller-based |
| Data access | `MySqlConnector` 2.6.2 — raw parameterised SQL, no ORM |
| Messaging | `Confluent.Kafka` 2.15.0 |
| Database | MySQL 8.0.40, database `reportingdb` |
| API docs | `Swashbuckle.AspNetCore` 6.6.2, generated from XML comments |
| Tests | xUnit 2.5.3 with `coverlet.collector`, hand-rolled fakes, no mocking library |

## Project Structure

```
database/
  migrations/           V01__create_job_projection.sql
docs/                   see Documentation
scripts/development/    apply_migrations.ps1
src/ReportingService/
  Controllers/          HealthController.cs, ReportsController.cs
  DTOs/                 JobsByStatusResponse.cs, JobStatusCountResponse.cs
  Messaging/
    Consumers/          JobCreatedConsumer.cs
    Contracts/          EventEnvelope.cs, JobCreatedPayload.cs
  Models/               JobProjection.cs, JobStatusCount.cs
  Repositories/         IDbConnectionFactory.cs, MySqlConnectionFactory.cs
                        IJobProjectionRepository.cs, JobProjectionRepository.cs
  Program.cs
terraform/              see Terraform Infrastructure
tests/ReportingService.Tests/
  Fakes/                FakeJobProjectionRepository.cs, FakeKafkaConsumer.cs
  UnitTests/            ReportsControllerTests.cs, JobCreatedConsumerTests.cs,
                        JobCreatedContractTests.cs
```

`Configuration/`, `Extensions/`, `Messaging/Producers/`, `Middleware/` and `Services/`
exist as scaffolding and are empty. `Messaging/Producers/` is expected to stay that way —
this service does not publish.

## Local Development

**Prerequisites:** .NET 8 SDK, Docker Desktop, and the
[`assms-platform-infrastructure`](../assms-platform-infrastructure) repository checked out
alongside this one — it owns the shared MySQL and Kafka containers.

1. **Start MySQL and Kafka**, following that repository's
   [local Kafka guide](../assms-platform-infrastructure/docs/kafka/local-development.md).
   Each stack is brought up from its own directory, so its `.env` is picked up:

   ```bash
   cd assms-platform-infrastructure/docker/local && docker compose up -d
   cd assms-platform-infrastructure/kafka/docker  && docker compose up -d
   ```

   MySQL's init scripts create `reportingdb` and the `reporting_svc` user on first run;
   the one-shot `topic-init` service creates `job-created` and exits 0.

2. **Apply the migrations.** From this repository's root:

   ```powershell
   .\scripts\development\apply_migrations.ps1 -Database reportingdb -User reporting_svc -Password 'ReportingLocalDev!23'
   ```

   Applied filenames are tracked in a `schema_migrations` table, so the script is safe to
   re-run and each migration runs exactly once.

3. **Create your local settings.** `appsettings.Development.json` is gitignored, so copy
   the tracked template and edit it if your credentials differ:

   ```powershell
   Copy-Item src\ReportingService\appsettings.Example.json src\ReportingService\appsettings.Development.json
   ```

4. **Run.**

   ```bash
   dotnet run --project src/ReportingService
   ```

   HTTP on `5238`, HTTPS on `7208`. Swagger UI is at `/swagger` in Development only.

The Kafka consumer starts with the host and reads from the **beginning** of the topic on
its first run, so a projection built after jobs already exist catches up rather than
starting blank. A broker that is down does not stop the API from serving.

## Environment Variables

Configuration is read through the standard ASP.NET Core providers, so every key below can
come from `appsettings.*.json` or from an environment variable using `__` in place of `:`.
`ConnectionStrings:default` and `Kafka:BootstrapServers` are **required** — the service
throws at startup if either is missing rather than falling back to a default that would
quietly point somewhere wrong.

| Key | Environment variable | Required | Purpose |
|---|---|---|---|
| `ConnectionStrings:default` | `ConnectionStrings__default` | yes | `reportingdb` connection string |
| `Kafka:BootstrapServers` | `Kafka__BootstrapServers` | yes | `localhost:9092` on the host, `kafka:29092` inside the Compose network |
| `Cors:AllowedOrigins` | `Cors__AllowedOrigins__0` | no | Browser origins allowed to call the API. **Absent means none are allowed**, not that all are |
| — | `ASPNETCORE_ENVIRONMENT` | no | `Development` enables Swagger |
| — | `MYSQL_PASSWORD` | no | Read by `apply_migrations.ps1` when `-Password` is not passed |

`.env.example` is still a placeholder; the values above are the ones that matter.

## Testing

```bash
dotnet test
```

47 unit tests, no database and no broker required.

| File | What it pins |
|---|---|
| `ReportsControllerTests` | The report and every invalid-filter case — bad dates, `from` after `to`, and no data returning **200 with an empty array rather than a 404** |
| `JobCreatedConsumerTests` | The consume loop: commit past a malformed message, seek back on a failed write, commit only *after* the write, and the consumer configuration itself |
| `JobCreatedContractTests` | That the locally redefined contract types read the event contract document's own example message |

`JobProjectionRepository` has **no unit tests**: `IDbConnectionFactory` returns a concrete
`MySqlConnection`, so a fake would verify a string rather than a query. Its SQL was checked
by hand against the local MySQL 8.0.40 container. An integration test against a disposable
database is the right home for it and is not yet written.

One consumer test deliberately waits out the loop's retry delay and takes about five
seconds. That is the test working, not hanging.

## Deployment

Staging and production run on a Terraform-provisioned Azure VM — see
[docs/deployment/staging-infrastructure.md](docs/deployment/staging-infrastructure.md) and
[Terraform Infrastructure](#terraform-infrastructure) below.

The `Dockerfile` and `.dockerignore` in this repository are **still placeholders**, and CI
does not deploy. Deployment is manual for now.

Two things this service needs wherever it runs that a stateless API does not: a reachable
Kafka broker, and its migrations applied before startup.

## Documentation

The API documents itself. `GenerateDocumentationFile` is set and Swashbuckle reads the
resulting XML, so the `///` comments on the controllers and DTOs *are* the API reference —
there is no second copy to keep in step. Swagger UI is served at `/swagger` in Development
only; the raw document is at `/swagger/v1/swagger.json`.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/Health` | Liveness. |
| `GET` | `/api/Health/db` | Liveness plus database reachability; **503** when the database is unavailable. |
| `GET` | `/api/reports/jobs-by-status` | Job counts grouped by status. Optional `?from=` and `?to=`, applied to when each job was **created upstream**. |

```json
{
  "statuses": [{ "status": "CREATED", "count": 12 }],
  "total": 12
}
```

`total` is the number of jobs counted, not the number of statuses. A status no job holds is
absent rather than reported as zero, because this service holds no list of what the
statuses are.

Invalid filters — a value that is not a date, or a `from` after its `to` — return **400**
with errors keyed by query parameter name. Filters that match nothing return **200** with
an empty array: a 404 would say the report does not exist, when what happened is that the
report ran and the answer was none.

### Design notes

| Area | Document |
|---|---|
| Read model, consumer and report | [docs/read-model/job-projection.md](docs/read-model/job-projection.md) |
| Event contract (cross-repository) | `assms-platform-infrastructure/docs/kafka/event-contracts.md` |
| Deployment | [docs/deployment/staging-infrastructure.md](docs/deployment/staging-infrastructure.md) |

## Database Ownership

This repository owns **`reportingdb` and nothing else**. The `reporting_svc` user is
granted privileges on `reportingdb` only and cannot read `customerdb`, `jobdb` or
`dispatchdb` even if a query tried.

`reportingdb` holds one table, `job_projection`, and it is a **projection rather than a
system of record**. Two consequences that make it unlike the other services' tables:

- **Its primary key is the upstream `job_id`**, not a local surrogate. Kafka delivers at
  least once, so that key is what makes a redelivered event update the row it already
  wrote instead of doubling every count in the report.
- **`status` and `region` carry no CHECK constraints**, unlike `jobs` in `jobdb`. A value
  the Job Service has already accepted and published must be absorbed here, not rejected —
  a CHECK would stall the consumer on the first event carrying a status added upstream, and
  every event queued behind it.

There are also no foreign keys: the ids in this table name rows in `jobdb`, a different
database owned by a different service, and MySQL cannot constrain across that boundary.

Schema changes are forward-only, one `V<n>__*.sql` file per change, applied by
`scripts/development/apply_migrations.ps1`. An applied migration is never edited.

## Kafka Responsibilities

**Consumes only. Produces nothing.**

| Topic | Consumer group | Status |
|---|---|---|
| `job-created` | `assms-reporting-job-created` | Implemented |
| `job-assigned` | `assms-reporting-job-assigned` | Assigned by the contract, **not implemented** |
| `job-status-changed` | `assms-reporting-job-status-changed` | Assigned by the contract, **not implemented** |

Group names follow the contract's `assms-<consuming-service>-<topic>` convention. The rule
it protects is that Dispatch and Reporting must never share a group on `job-created`: a
shared group would make Kafka hand each event to only one of them, and both need every
event independently.

The consumer commits offsets **manually, after the database write**. Committing first would
lose a job from the report whenever the process died in between; committing last risks
writing the row twice, which the idempotent upsert makes harmless. Deserialization failures
and write failures are handled differently on purpose — a malformed message is committed
past, a failed write is read again. The reasoning is in
[docs/read-model/job-projection.md](docs/read-model/job-projection.md).

The envelope and payload types in `Messaging/Contracts/` are a **redefinition** of what the
Job Service publishes, not a shared package. That duplication is deliberate and its cost is
that the contract document, not either copy, is what both are kept in step with.

## Terraform Infrastructure

This repository owns only the Reporting Service VM, NIC, service-specific NSG, and optional public IP. Shared resource-group and services-subnet values are consumed from the platform Terraform remote state or supplied through explicit overrides; this repository does not recreate the shared VNet or subnets.

## Continuous Integration

GitHub Actions runs on pull requests targeting `dev` or `main` and pushes to `dev` or `main`. CI validates Terraform formatting and both environment roots, then restores and builds the .NET 8 solution in Release mode and runs its tests with the configured XPlat coverage collector. Mandatory failures fail CI; test results and coverage are retained as workflow artifacts. No deployment occurs from this workflow.

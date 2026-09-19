# US-08 — Jobs-by-Technician Dynamic Report

## Endpoint

`GET /api/reports/jobs-by-technician` returns `technicians` and `total` from Reporting Service's local `reportingdb` read model.

Optional query parameters:

- `from`: RFC 3339 timestamp; inclusive assignment-time lower bound.
- `to`: RFC 3339 timestamp; exclusive assignment-time upper bound.
- `region`: an exact normalized region value, such as `WESTERN`.

All supplied filters use **AND** semantics. `from` must be earlier than `to`. Valid filters with no matching assignments return `200 OK` with `technicians: []` and `total: 0`; invalid timestamps, ranges, or regions return `400 Bad Request` with `ValidationProblemDetails` keyed by the invalid filter.

## Read-model boundary and idempotency

The report reads only `job_projection` joined with `job_assignment_projection` in `reportingdb`. `JobAssignedConsumer` projects Kafka `job-assigned` events using `INSERT ... ON DUPLICATE KEY UPDATE` keyed on `job_id`, so duplicate delivery updates the existing assignment fact and cannot increase the report count.

The SQL applies `a.assigned_at >= @from` and `a.assigned_at < @to`, which lets adjacent report windows meet at one timestamp without double-counting an assignment.

## Manager UI

`/reports/jobs-by-technician` is Manager-only. It provides RFC 3339 UTC range fields, a normalized-region selector, dynamic grouped counts, and separate no-data and invalid-filter states.

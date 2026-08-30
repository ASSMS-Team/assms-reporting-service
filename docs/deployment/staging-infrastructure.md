# Reporting Service Staging Infrastructure

## Current Infrastructure

| Item | Value |
|---|---|
| VM | `vm-assms-reporting-staging` |
| Region | Central India |
| SKU / architecture | `Standard_B2pls_v2`, Arm64, 2 vCPU / 4 GiB |
| OS | Ubuntu 22.04 Arm64 with Standard_LRS OS disk |
| Private network | Secondary services subnet `10.30.1.0/24`; assigned private IP `10.30.1.5` |
| Public IP | Static Standard resource exists for future controlled access |
| State key | `reporting-service/staging.tfstate` |
| Platform dependency | Reads `platform/staging.tfstate`, including `secondary_services_subnet_id` |

Password authentication is disabled. The NSG has no custom inbound rules; no SSH, public application, MySQL, or Kafka access is exposed. Azure default deny-inbound remains effective. Cross-region peering and the MySQL private-DNS link support future private connectivity to Kafka and MySQL.

## Deployment Verification

| Check | Result |
|---|---|
| Application source SHA | `efc229f6034c4719d6eec47a14a9f281b08ed5e7` (developer CI manually verified PASS) |
| DevOps branch / commit | `ASSMS-4-us-04-staging-deployment` / `6bdcf08` |
| Image | `assms-reporting-service:efc229f6034c4719d6eec47a14a9f281b08ed5e7` (`linux/arm64`) |
| Database migration | `database/migrations/V01__create_job_projection.sql` — PASS |
| Runtime | Docker container `assms-reporting-service`, loopback-only `127.0.0.1:8080` |
| HTTPS endpoint | `https://assms-reporting-staging-45ff260826.centralindia.cloudapp.azure.com` |
| Health / DB health | `GET /api/health` 200; `GET /api/health/db` 200 |

Manual API verification passed for `GET /api/reports/jobs-by-status`. The unfiltered report returned the expected response schema; the supported `from`/`to` date filter succeeded; and a historic no-data range returned 200 with an empty `statuses` array and `total: 0`.

`REPORT_GROUPED_DATA_VERIFICATION_DEFERRED_WITH_KAFKA`: no legitimate event-fed projection data was present and Kafka remains deferred. No projection rows were manually inserted.

`KAFKA_SPRINT1_DEFERRED`: the Kafka VM remains deallocated and consumer/event processing was not claimed as verified. Nginx terminates HTTPS and proxies only to loopback. TCP 8080, MySQL, Kafka, and SSH are not publicly exposed; no secrets are stored in this document.

## ARM64 Note

The inspected Reporting project targets .NET 8 and uses managed dependencies without current x64-only runtime settings. Its Dockerfile is still a placeholder. Future deployment requires a multi-architecture .NET image and an Arm64 Docker build/test.

## Future Deployment Work

Start the VM, configure restricted deployment access, implement/test a Dockerfile, deploy the API, configure reviewed private MySQL/Kafka settings, and verify health checks. No runtime secrets, API port, or Kafka consumer configuration has been applied.

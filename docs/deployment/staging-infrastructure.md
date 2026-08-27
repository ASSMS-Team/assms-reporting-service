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

## Application Status

The Reporting read model, reports API, Docker runtime, and Kafka event consumers are **not deployed**. The VM is currently deallocated for staging cost control.

## ARM64 Note

The inspected Reporting project targets .NET 8 and uses managed dependencies without current x64-only runtime settings. Its Dockerfile is still a placeholder. Future deployment requires a multi-architecture .NET image and an Arm64 Docker build/test.

## Future Deployment Work

Start the VM, configure restricted deployment access, implement/test a Dockerfile, deploy the API, configure reviewed private MySQL/Kafka settings, and verify health checks. No runtime secrets, API port, or Kafka consumer configuration has been applied.

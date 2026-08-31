# Reporting Service Staging CD

The Reporting CI workflow calls reusable CD only after Terraform validation and .NET build/test succeed on a `dev` push. It checks out and tags the exact tested `github.sha`, builds `linux/arm64`, and replaces only `assms-reporting-service`.

GitHub OIDC authenticates to the scoped staging identity. A temporary runner IPv4 `/32` TCP 22 rule is removed with `always()`. SSH checks `REPORTING_VM_SSH_KNOWN_HOSTS`, loads the image archive, and leaves `/etc/assms/reporting.env` untouched. The service stays loopback-only on `127.0.0.1:8080`; public HTTPS health and DB-health must return 200.

Required Actions variables: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `REPORTING_API_URL`, `REPORTING_NSG_NAME`, `REPORTING_TEMP_SSH_RULE_PRIORITY`, `REPORTING_VM_HOST`, `REPORTING_VM_SSH_USERNAME`, and `REPORTING_VM_SSH_KNOWN_HOSTS`. Required secret: `REPORTING_DEPLOY_SSH_PRIVATE_KEY`.

Database migration remains operator-controlled. Kafka is Sprint 1 deferred; this workflow does not start Kafka or claim consumer verification. Roll back by redeploying a previously CI-validated `dev` SHA.

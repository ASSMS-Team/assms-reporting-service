USE reportingdb;

-- ASSMS-38 / US-12 / TASK-32: Job completion dynamic report projection.
-- Populated by JobStatusChanged events where new_status = 'COMPLETED'.
-- Using job_id as the primary key guarantees that duplicate or redelivered
-- Kafka events are idempotent and do not inflate completion totals.
CREATE TABLE IF NOT EXISTS job_completion_projection (
    job_id                CHAR(36)    NOT NULL,
    job_reference         VARCHAR(20) NOT NULL,
    technician_id         CHAR(36)    NULL,
    technician_reference  VARCHAR(30) NULL,
    completed_at          TIMESTAMP   NOT NULL,
    projected_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (job_id),
    KEY idx_job_completion_completed_at (completed_at)
);

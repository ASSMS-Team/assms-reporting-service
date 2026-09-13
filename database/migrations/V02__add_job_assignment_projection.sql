-- ASSMS-30: Dispatch owns assignments. This separate projection makes the
-- read model safe when JobAssigned reaches Reporting before JobCreated: each
-- consumer upserts its own fact and the report joins them when both exist.
CREATE TABLE IF NOT EXISTS job_assignment_projection (
    job_id                CHAR(36)    NOT NULL,
    technician_id         CHAR(36)    NOT NULL,
    technician_reference  VARCHAR(30) NOT NULL,
    assigned_at           TIMESTAMP   NOT NULL,
    projected_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (job_id),
    KEY idx_assignment_projection_technician (technician_id)
);

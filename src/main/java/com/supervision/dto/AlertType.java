package com.supervision.dto;

public enum AlertType {
    // --- Stambia (existant) ---
    ERROR,
    SLA_BREACH,
    TIMEOUT,
    SESSION_NOT_TRIGGERED,

    // --- Airflow (nouveaux) ---
    TASK_FAILED,
    DAG_ABORTED,
    UPSTREAM_FAILED,
    RETRY_EXHAUSTED,
    REPEATED_FAILURE,
    MANUAL_OVERRIDE,
    LATE_START,
    DAG_NOT_TRIGGERED,
    STUCK_RUNNING
}
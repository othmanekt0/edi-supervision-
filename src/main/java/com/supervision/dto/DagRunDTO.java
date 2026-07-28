package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class DagRunDTO {
    private String dagId;
    private String runId;
    private String status;       // état brut Airflow (success, failed, running…)
    private String triggerType;  // run_type : scheduled, manual, backfill…
    private LocalDateTime startDatetime;
    private LocalDateTime endDatetime;
}
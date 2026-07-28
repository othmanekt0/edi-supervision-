package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class TaskInstanceDTO {
    private String dagId;
    private String runId;
    private String taskId;
    private String state;        // success, failed, upstream_failed, skipped…
    private LocalDateTime startDatetime;
    private LocalDateTime endDatetime;
}
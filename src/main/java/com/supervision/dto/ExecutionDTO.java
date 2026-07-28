package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.ZonedDateTime;

@Data
@Builder
public class ExecutionDTO {
    private String sourceExecutionRef;   // SESSION_ID
    private String interfaceCode;        // SESSION_NAME → lookup DIM_INTERFACE
    private Integer sourceId;            // 1 = Stambia
    private String environment;          // prod, preprod, dev
    private String status;               // SUCCESS, FAILURE, RUNNING...
    private String triggerType;          // SCHEDULED, MANUAL, API
    private ZonedDateTime startDatetime;
    private ZonedDateTime endDatetime;
    private Long durationSeconds;
    private String serverName;
    private String errorMessage;
}
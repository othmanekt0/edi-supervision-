package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.ZonedDateTime;

@Data
@Builder
public class StepDTO {
    private String parentExecutionRef;   // PARENT_SESSION_ID
    private String stepExecutionRef;     // SESSION_ID de l'étape
    private String stepName;             // SESSION_NAME (chemin complet)
    private String status;
    private ZonedDateTime startDatetime;
    private ZonedDateTime endDatetime;
    private Long durationSeconds;
    private String errorMessage;
}
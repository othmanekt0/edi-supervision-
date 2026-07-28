package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class SlaMissDTO {
    private String dagId;
    private LocalDateTime executionDate;
    private LocalDateTime triggeredAt;
    private String description;
}
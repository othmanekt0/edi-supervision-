package com.supervision.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DagDTO {
    private String dagId;
    private String displayName;
    private Boolean isPaused;
    private String scheduleInterval;
    private String fileloc;       // chemin du fichier DAG sur le filesystem
}
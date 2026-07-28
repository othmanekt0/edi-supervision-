package com.supervision.dto;

import lombok.Builder;
import lombok.Data;
import java.time.ZonedDateTime;

@Data
@Builder
public class ActionDTO {
    private String sourceStepRef;   // act_id
    private String sessId;          // sess_id — session ou sous-session de rattachement directe
    private String parentRef;       // ⚠️ vient de la colonne act_father_engine_id (PAS act_parent_iter, malgré son nom !)
    // Confirmé sur données réelles du dump + information_schema.columns le 13/07/2026 :
    // act_father_engine_id = vide (racine, via sessId) ou act_id du parent.
    // act_parent_iter, malgré son nom, ne vaut que 0/1 et n'est PAS la clé de hiérarchie.
    private String stepName;        // act_real_name (nom court)
    private String actType;         // Code | Process
    private String status;
    private ZonedDateTime startDatetime;
    private ZonedDateTime endDatetime;
    private String errorMessage;
}
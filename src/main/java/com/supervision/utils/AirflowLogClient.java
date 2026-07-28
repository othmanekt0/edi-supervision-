package com.supervision.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Accès à l'API REST Airflow pour récupérer les logs de task en échec.
 * Fusionne AirflowRestClient + AirflowLogService du projet clients1.
 */
@Component
@Slf4j
public class AirflowLogClient {

    private static final Pattern ERROR_LINE_PATTERN =
            Pattern.compile("(?i).*(error|exception|traceback|failed).*");

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${airflow.rest.base-url}")
    private String baseUrl;

    @Value("${airflow.rest.username}")
    private String username;

    @Value("${airflow.rest.password}")
    private String password;

    /**
     * Récupère le log brut d'une task via l'API REST Airflow.
     * Retourne null si le log est introuvable ou l'API indisponible.
     */
    public String getTaskLog(String dagId, String runId, String taskId, int tryNumber) {
        String url = baseUrl
                + "/api/v1/dags/" + dagId
                + "/dagRuns/" + runId
                + "/taskInstances/" + taskId
                + "/logs/" + tryNumber;

        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(username, password);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, String.class);
            return response.getBody();
        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Log introuvable pour {}/{}/{} try={}", dagId, runId, taskId, tryNumber);
            return null;
        } catch (Exception e) {
            log.warn("API Airflow indisponible pour {}/{}/{} : {}", dagId, runId, taskId, e.getMessage());
            return null;
        }
    }

    /**
     * Extrait la première ligne significative d'erreur du log brut.
     * Retourne null si aucune ligne ne matche.
     */
    public String extractErrorLine(String logContent) {
        if (logContent == null || logContent.isBlank()) return null;

        for (String line : logContent.split("\n")) {
            Matcher matcher = ERROR_LINE_PATTERN.matcher(line);
            if (matcher.matches()) {
                // Tronqué à 500 chars pour respecter la contrainte TEXT de fact_execution_step
                return line.trim().length() > 500
                        ? line.trim().substring(0, 500)
                        : line.trim();
            }
        }
        return null;
    }
}
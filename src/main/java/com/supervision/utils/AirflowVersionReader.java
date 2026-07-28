package com.supervision.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Component
@Slf4j
public class AirflowVersionReader {

    @Value("${airflow.docker.container-name}")
    private String containerName;

    private String cachedVersion;

    public synchronized String getVersion() {
        if (cachedVersion != null) return cachedVersion;
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "airflow", "version");
            Process p = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder lastLine = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    lastLine.setLength(0);
                    lastLine.append(line.trim());
                }
            }
            p.waitFor();
            cachedVersion = !lastLine.isEmpty() ? lastLine.toString() : "inconnue";
        } catch (Exception e) {
            log.warn("Impossible de récupérer la version Airflow : {}", e.getMessage());
            cachedVersion = "inconnue";
        }
        return cachedVersion;
    }
}
package com.supervision.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class DagFileReader {

    private static final Pattern TIMEOUT_PATTERN = Pattern.compile(
            "dagrun_timeout\\s*=\\s*timedelta\\(([^)]*)\\)");
    private static final Pattern KV_PATTERN = Pattern.compile("(\\w+)\\s*=\\s*(\\d+)");

    @Value("${airflow.docker.container-name}")
    private String containerName;

    public Integer readDagrunTimeoutSeconds(String fileloc) {
        if (fileloc == null || fileloc.isBlank()) return null;
        String content = readFileFromContainer(fileloc);
        if (content == null) return null;

        Matcher m = TIMEOUT_PATTERN.matcher(content);
        if (!m.find()) return null;

        String args = m.group(1);
        int totalSeconds = 0;
        Matcher kv = KV_PATTERN.matcher(args);
        while (kv.find()) {
            String unit = kv.group(1);
            int value   = Integer.parseInt(kv.group(2));
            totalSeconds += switch (unit) {
                case "days"    -> value * 86400;
                case "hours"   -> value * 3600;
                case "minutes" -> value * 60;
                case "seconds" -> value;
                default        -> 0;
            };
        }
        return totalSeconds > 0 ? totalSeconds : null;
    }

    private String readFileFromContainer(String fileloc) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", containerName, "cat", fileloc);
            Process p = pb.start();
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
            boolean finished = p.waitFor(10, TimeUnit.SECONDS);
            if (!finished || p.exitValue() != 0) {
                log.warn("Impossible de lire {} dans le conteneur {}", fileloc, containerName);
                return null;
            }
            return sb.toString();
        } catch (Exception e) {
            log.warn("Erreur lecture fichier DAG {} : {}", fileloc, e.getMessage());
            return null;
        }
    }
}
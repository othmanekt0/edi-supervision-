package com.supervision.consumer;

import com.supervision.loader.AirflowLoader;
import com.supervision.transformer.AirflowTransformer;
import com.supervision.transformer.AirflowTransformer.DebeziumEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AirflowConsumer {

    private final AirflowTransformer transformer;
    private final AirflowLoader      loader;

    @KafkaListener(
            topicPattern = "${app.kafka.debezium.topic-pattern}",
            groupId      = "clients-cdc-consumer"
    )
    public void onDebeziumEvent(
            String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        try {
            DebeziumEvent event = transformer.parse(payload, topic);

            if (event == null) return;

            if (event.shouldIgnore()) {
                log.debug("Événement ignoré (op={} table={})", event.operation(), event.tableName());
                return;
            }

            switch (event.tableName().toLowerCase()) {
                case "dag_run" ->
                        loader.upsertExecutionFromDagRun(
                                transformer.toDagRunDTO(event.data()));

                case "task_instance" ->
                        loader.upsertStepFromTaskInstance(
                                transformer.toTaskInstanceDTO(event.data()));

                case "dag" -> {
                    // Ignorer les updates purement techniques (last_parsed_time, next_dagrun...)
                    if ("u".equals(event.operation()) &&
                            transformer.isDagTechnicalUpdateOnly(event.before(), event.data())) {
                        log.debug("Update dag technique ignoré : {}",
                                event.data().path("dag_id").asText());
                        break;
                    }
                    loader.upsertInterfaceFromDagEvent(transformer.toDagDTO(event.data()));
                }

                case "sla_miss" ->
                        loader.handleSlaMiss(
                                transformer.toSlaMissDTO(event.data()));

                default ->
                        log.debug("Table non gérée : {}", event.tableName());
            }

        } catch (IllegalArgumentException e) {
            log.warn("Payload Airflow invalide sur topic {} : {}", topic, e.getMessage());
        } catch (Exception e) {
            log.error("Erreur inattendue sur topic {} : {}", topic, e.getMessage(), e);
        }
    }
}
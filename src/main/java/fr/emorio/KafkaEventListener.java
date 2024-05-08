package fr.emorio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.HashMap;
import java.util.Map;

public class KafkaEventListener implements EventListenerProvider {
    private KafkaProducer<String, String> producer;

    public KafkaEventListener(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void onEvent(Event event) {
        switch (event.getType()) {
            case REGISTER -> onRegisterEvent(event);
            default -> {
            }
        }
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean b) {
        //No particular action for admin events, for now
    }

    private void onRegisterEvent(Event event) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("userId", event.getUserId());
        properties.put("email", event.getDetails().get("email"));
        properties.put("ipAddress", event.getIpAddress());
        properties.put("time", event.getTime());
        producer.send(new ProducerRecord("USER_REGISTERED", properties.toString()));
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}

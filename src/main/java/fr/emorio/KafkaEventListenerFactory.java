package fr.emorio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Properties;

public class KafkaEventListenerFactory implements EventListenerProviderFactory {
    private Properties properties;


    @Override
    public EventListenerProvider create(KeycloakSession keycloakSession) {
        return new KafkaEventListener(new KafkaProducer<>(properties));
    }

    @Override
    public void init(Config.Scope scope) throws RuntimeException {
        properties = new Properties();
        String kafkaServer = scope.get("kafkaServer");
        if (kafkaServer == null) {
            throw new IllegalArgumentException("Kafka server is not configured (kafkaServer)");
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    @Override
    public void postInit(KeycloakSessionFactory keycloakSessionFactory) {
        //not needed
    }

    @Override
    public void close() {
        // not needed
    }

    @Override
    public String getId() {
        return "kafkaEventListener";
    }
}

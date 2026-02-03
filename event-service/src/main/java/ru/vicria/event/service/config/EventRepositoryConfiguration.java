package ru.vicria.event.service.config;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import ru.vicria.event.service.mongo.CollectionInsertListenerImpl;
import ru.vicria.event.service.mongo.EventRepositoryImpl;

import java.util.concurrent.Executors;

import static ru.vicria.event.service.EventApp.CLIENT_ID;

public class EventRepositoryConfiguration {

    private final EventRepositoryImpl repository;

    public EventRepositoryConfiguration() {
        var clientSettings = MongoClientSettings.builder()
                .applicationName(CLIENT_ID)
                //TODO: check different settings
                .build();
        var mongoClient = MongoClients.create(clientSettings);
        var mongoDataBase = mongoClient.getDatabase("event-example");
        this.repository = new EventRepositoryImpl(
                mongoDataBase,
                "event-example",
                new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor()));
    }

    public EventRepositoryImpl getRepository() {
        return repository;
    }
}

package ru.vicria.event.service.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import ru.vicria.event.service.mongo.CollectionInsertListenerImpl;
import ru.vicria.event.service.mongo.EventRepositoryImpl;

import java.util.concurrent.Executors;

import static ru.vicria.event.service.EventApp.CLIENT_ID;

public class EventRepositoryConfiguration {

    private static final String CONNECTION_STRING = "mongodb://mongo:27017";
    private static final String COLLECTION_NAME = "event-example";

    private final EventRepositoryImpl repository;

    public EventRepositoryConfiguration() {
        var clientSettings = MongoClientSettings.builder()
                //TODO: check different settings
                .applicationName(CLIENT_ID)
                .applyConnectionString(new ConnectionString(CONNECTION_STRING))
                .build();

        var mongoClient = MongoClients.create(clientSettings);
        var mongoDataBase = mongoClient.getDatabase(COLLECTION_NAME);

        this.repository = new EventRepositoryImpl(
                mongoDataBase,
                COLLECTION_NAME,
                new CollectionInsertListenerImpl(
                        Executors.newSingleThreadScheduledExecutor()
                )
        );
    }

    public EventRepositoryImpl getRepository() {
        return repository;
    }
}

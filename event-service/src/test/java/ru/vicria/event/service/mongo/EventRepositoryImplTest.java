package ru.vicria.event.service.mongo;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vicria.event.service.api.Event;

import java.util.Collections;
import java.util.concurrent.Executors;

class EventRepositoryImplTest extends BaseMongoTest {

    private final Logger logger = LoggerFactory.getLogger(EventRepositoryImplTest.class);

    @Test
    void listenSince() {
        withMongo(mongo -> {
            var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
            var repository = new EventRepositoryImpl(mongo, "123", listener);
            var timeMillis = System.currentTimeMillis();
            repository.listenSince(timeMillis)
                    .doOnNext(event -> logger.info("processing msg {}", event.getMessage()))
                    .subscribe();

            var event = Event.newBuilder()
                    .setNotificationTime(timeMillis)
                    .setMessage("Skip")
                    .build();
            repository.insert(Collections.singletonList(event));

            var event2 = Event.newBuilder()
                    .setNotificationTime(timeMillis + 1)
                    .setMessage("Show")
                    .build();
            repository.insert(Collections.singletonList(event2));
        });
    }
}
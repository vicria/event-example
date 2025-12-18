package ru.vicria.event.service.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import ru.vicria.event.service.api.Event;

import static ru.vicria.event.service.mongo.EventField.MESSAGE;
import static ru.vicria.event.service.mongo.EventField.NOTIFICATION_TS;

public class EventRepositoryImpl {

    public static final Logger logger = LoggerFactory.getLogger(EventRepositoryImpl.class);
    private final MongoCollection<Document> mongoCollection;
    private final CollectionInsertListener insertListener;

    public EventRepositoryImpl(
            MongoDatabase mongoDatabase,
            String collectionName,
            CollectionInsertListener insertListener
    ) {
        mongoCollection = mongoDatabase.getCollection(collectionName.toLowerCase());
        mongoCollection.createIndex(
                Indexes.ascending(MESSAGE.name(), NOTIFICATION_TS.name()),
                new IndexOptions().unique(true).name("event_name_idx"));
        this.insertListener = insertListener;
        insertListener.start(mongoCollection);
    }

    public Flux<Event> listenSince(long timestamp) {
        var result = mongoCollection
                .find(Filters.gt(NOTIFICATION_TS.name(), timestamp))
                .sort(Sorts.descending(NOTIFICATION_TS.name()));

        return Flux.merge(
                Flux.fromIterable(result),
                insertListener.insertedDocumentFlux().filter(doc -> NOTIFICATION_TS.from(doc) > timestamp)
        ).map(EventParser::parse);
    }
}

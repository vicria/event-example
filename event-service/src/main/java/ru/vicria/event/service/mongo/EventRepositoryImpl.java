package ru.vicria.event.service.mongo;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vicria.event.service.api.AnalyticsEvent;
import ru.vicria.event.service.api.Event;
import ru.vicria.event.service.api.TailEventsRequest;

import java.util.Collections;
import java.util.List;

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

        mongoCollection.createIndex(
                Indexes.ascending(NOTIFICATION_TS.name()),
                new IndexOptions().name("notification_ts_idx")
        );

        this.insertListener = insertListener;
        insertListener.start(mongoCollection);
    }

    // TODO: check what if not merge?
    // TODO: what if Flux isn't infinite and these is no insertListener flux?
    public Flux<Event> listenSince(long timestamp) {
        var result = mongoCollection
                .find(Filters.gt(NOTIFICATION_TS.name(), timestamp))
                .sort(Sorts.descending(NOTIFICATION_TS.name()));
        Flux<Document> queryFlux = Flux.fromIterable(result);

        Flux<Document> liveFlux = insertListener.insertedDocumentFlux()
                .filter(doc -> NOTIFICATION_TS.from(doc) > timestamp);

        return Flux.merge(queryFlux, liveFlux)
                .map(EventParser::parse)
                .distinct(e -> e.getMessage() + "|" + e.getNotificationTime());
    }

    public void insert(List<Event> events) {
        var operations = events.stream()
                .map(event -> {
                    var filter = Filters.and(
                            Filters.eq(MESSAGE.name(), event.getMessage()),
                            Filters.eq(NOTIFICATION_TS.name(), event.getNotificationTime())
                    );
                    return new ReplaceOneModel<>(
                            filter,
                            EventParser.toDocument(event),
                            new ReplaceOptions().upsert(true)
                    );
                })
                .toList();

        try {
            mongoCollection.bulkWrite(operations);
//            if (result.wasAcknowledged()) {
//                //we can do emit FAIL_FAST
//            }
        } catch (MongoBulkWriteException e) {
            if (e.getWriteErrors().stream().findAny().isPresent()
                    && e.getWriteErrors().stream().findAny().get().getCode() == 1100) {
                logger.info("Duplicate key");
            } else {
                throw e;
            }
        }
    }

    public Mono<Event> saveOne(AnalyticsEvent analyticsEvent) {
        Event event = analyticsEvent.getEvent();
        Document document = EventParser.toDocument(event);

        return Mono.fromCallable(() -> {
            mongoCollection.insertOne(document);
            return event;
        }).subscribeOn(Schedulers.boundedElastic());

    }

    public Flux<Event> tailThenListen(TailEventsRequest req) {
        long minNotificationTime = req.getMinNotificationTime();
        int tailSize = req.getTailSize();
        int maxEvents = req.getMaxEvents();

        Mono<List<Event>> tailListMono =
                tailSize <= 0
                        ? Mono.just(List.of())
                        : fetchTailDescending(minNotificationTime, tailSize)
                        .collectList()
                        .map(list -> {
                            Collections.reverse(list);
                            return list;
                        });

        Flux<Event> combined = tailListMono.flatMapMany(tailList -> {
            long watermark = tailList.isEmpty()
                    ? minNotificationTime
                    : tailList.get(tailList.size() - 1).getNotificationTime();

            Flux<Event> tailFlux = Flux.fromIterable(tailList);

            Flux<Event> liveFlux = insertListener.insertedDocumentFlux()
                    .filter(doc -> NOTIFICATION_TS.from(doc) > watermark)
                    .map(EventParser::parse);

            return tailFlux.concatWith(liveFlux);
        });

        if (maxEvents > 0) {
            return combined.take(maxEvents);
        }

        return combined;
    }

    private Flux<Event> fetchTailDescending(long minNotificationTime, int tailList) {
        FindIterable<Document> documents = mongoCollection.find(Filters.gt(NOTIFICATION_TS.name(), minNotificationTime))
                .sort(Sorts.descending(NOTIFICATION_TS.name()))
                .limit(tailList);

        return Flux.fromIterable(documents::iterator)
                .subscribeOn(Schedulers.boundedElastic())
                .map(EventParser::parse);
    }
}

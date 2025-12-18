package ru.vicria.event.service.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import reactor.core.publisher.Flux;

public interface CollectionInsertListener {
    void start(MongoCollection<Document> collection);

    Flux<Document> insertedDocumentFlux();
}

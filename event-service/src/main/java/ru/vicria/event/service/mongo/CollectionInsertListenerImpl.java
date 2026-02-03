package ru.vicria.event.service.mongo;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CollectionInsertListenerImpl implements CollectionInsertListener {
    private static final Logger logger = LoggerFactory.getLogger(CollectionInsertListenerImpl.class);

    private static final Long DELAY = 10L;

    private final ScheduledExecutorService executorService;
    // TODO: try with different sinks (no buffers) on no sinks at all
    private final Sinks.Many<Document> publisher = Sinks.many().multicast().onBackpressureBuffer(1024);

    private final AtomicReference<BsonTimestamp> startTsRef = new AtomicReference<>();
    private final AtomicReference<BsonDocument> resumeTokenRef = new AtomicReference<>();
    private final AtomicReference<MongoCollection<Document>> collectionRef = new AtomicReference<>();

    public CollectionInsertListenerImpl(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void start(MongoCollection<Document> collection) {
        startTsRef.set(currentTs());
        if (!collectionRef.compareAndSet(null, collection)) {
            throw new IllegalStateException("Listener is already associated");
        }
        final var changeStream = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
        executorService.submit(() -> listenChanges(changeStream));
    }

    private void listenChanges(ChangeStreamIterable<Document> changeStream) {
        try {
            changeStream.forEach(doc ->{
                resumeTokenRef.set(doc.getResumeToken());
                var isInsert = doc.getOperationType() == OperationType.INSERT;
                if (isInsert && doc.getFullDocument() != null) {
                    logger.info("Received insert change: {} for collection {}", doc.getFullDocument(), doc.getNamespace());
                    publisher.emitNext(doc.getFullDocument(), Sinks.EmitFailureHandler.FAIL_FAST);
                }
            });
        } catch (MongoCommandException mongoEx) {
            logger.error("change stream failed with MongoCommandException", mongoEx);
            if (mongoEx.getErrorCode() == 286) {
                resumeTokenRef.set(null);
                publisher.emitError(new IllegalStateException("Can not resume change"), Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
                resumeChangeStream();
            }
        } catch (Exception e) {
            logger.error("change stream failed", e);
            resumeChangeStream();
        }
    }

    private void resumeChangeStream() {
        try {
            var resumeToken = resumeTokenRef.get();
            var startTs = startTsRef.get();
            var collection = collectionRef.get();
            logger.error("change stream failed, going to reconnect for resumeToken={} or ts={}", resumeToken, startTs);
            var resumedChangeStream = resumeToken != null
                    ? collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken)
                    : collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).startAtOperationTime(startTs);
            executorService.schedule(()-> listenChanges(resumedChangeStream), DELAY, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("failed to resume for resumeToken={} or ts={}", resumeTokenRef.get(), startTsRef.get(), e);
            executorService.schedule(this::resumeChangeStream, DELAY, TimeUnit.SECONDS);
        }
    }

    private BsonTimestamp currentTs() {
        return new BsonTimestamp(Math.toIntExact(Instant.now().getEpochSecond()), 0);
    }

    @Override
    public Flux<Document> insertedDocumentFlux(){
        return publisher.asFlux();
    }
}

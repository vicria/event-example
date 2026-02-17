package ru.vicria.event.service.api;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import ru.vicria.event.service.mongo.CollectionInsertListenerImpl;
import ru.vicria.event.service.mongo.EventParser;
import ru.vicria.event.service.mongo.EventRepositoryImpl;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static ru.vicria.event.service.mongo.EventField.NOTIFICATION_TS;


@ExtendWith(MockitoExtension.class)
class EventServiceImplComponentTest {

    private static final String COLLECTION_NAME = "event-example";
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(1);
    private static final long MIN_NOTIFICATION_TIME = 1_000_000L;

    private EventServiceImpl eventService;

    private ExecutorService pool;

    private ScheduledExecutorService listenerExecutor;

    private CompletableFuture<Consumer<ChangeStreamDocument<Document>>> consumerFuture;

    @Mock
    private MongoDatabase mongoDatabase;

    @Mock
    private MongoCollection<Document> mongoCollection;

    @Mock
    private ChangeStreamIterable<Document> changeStreamIterable;

    @Mock
    private FindIterable<Document> findIterable;

    @Mock
    private MongoCursor<Document> cursor;

    @BeforeEach
    void setUp() {
        consumerFuture = new CompletableFuture<>();

        pool = Executors.newSingleThreadExecutor();

        EventRepositoryImpl eventRepository = setUpRepository();
        eventService = new EventServiceImpl(eventRepository);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        pool.shutdownNow();
        if (listenerExecutor != null) {
            listenerExecutor.shutdownNow();
            listenerExecutor.awaitTermination(200, TimeUnit.MILLISECONDS);
        }
    }

    private EventRepositoryImpl setUpRepository() {
        listenerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mongo-change-listener-test");
            t.setDaemon(true);
            return t;
        });

        given(mongoDatabase.getCollection(COLLECTION_NAME.toLowerCase()))
                .willReturn(mongoCollection);
        given(mongoCollection.createIndex(any(Bson.class), any(IndexOptions.class)))
                .willReturn("new-index");
        given(mongoCollection.watch())
                .willReturn(changeStreamIterable);
        given(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP))
                .willReturn(changeStreamIterable);

        doAnswer(inv -> {
            consumerFuture.complete(inv.getArgument(0));
            return null;
        }).when(changeStreamIterable).forEach(any(Consumer.class));

        return new EventRepositoryImpl(
                mongoDatabase,
                COLLECTION_NAME,
                new CollectionInsertListenerImpl(listenerExecutor)
        );
    }


    @Test
    void shouldEmitTailAndLiveEvents_InCorrectOrder_OnHappyPath() throws InterruptedException, ExecutionException, TimeoutException {
        // given
        TailEventsRequest req = buildTailEventsRequest(100);

        Document tail1 = buildDocument(MIN_NOTIFICATION_TIME + 1, "tail1");
        Document tail2 = buildDocument(MIN_NOTIFICATION_TIME + 2, "tail2");
        Document inserted1 = buildDocument(MIN_NOTIFICATION_TIME + 100, "a");
        Document inserted2 = buildDocument(MIN_NOTIFICATION_TIME + 200, "b");

        givenTail(tail2, tail1);

        ChangeStreamDocument<Document> ev1 = insertEvent(inserted1);
        ChangeStreamDocument<Document> ev2  = insertEvent(inserted2);

        // when
        Future<List<Event>> future = pool.submit(() ->
                eventService.tailEvents(Mono.just(req))
                        .take(4)
                        .timeout(TEST_TIMEOUT)
                        .collectList()
                        .block()
        );

        Consumer<ChangeStreamDocument<Document>> consumer = awaitConsumer();
        consumer.accept(ev1);
        consumer.accept(ev2);

        List<Event> eventList = future.get(2, TimeUnit.SECONDS);

        // then
        then(eventList)
                .hasSize(4)
                .containsExactly(
                        EventParser.parse(tail1),
                        EventParser.parse(tail2),
                        EventParser.parse(inserted1),
                        EventParser.parse(inserted2)
                );
    }

    @Test
    void shouldSkipLiveEventsAtOrBeforeWatermark() throws Exception {
        TailEventsRequest req = buildTailEventsRequest(100);

        Document tail1 = buildDocument(MIN_NOTIFICATION_TIME + 1, "tail1");
        Document tail2 = buildDocument(MIN_NOTIFICATION_TIME + 2, "tail2");   // watermark will become tail2.ts = min + 2
        Document liveOld = buildDocument(MIN_NOTIFICATION_TIME + 1, "old");   // < watermark
        Document liveEq = buildDocument(MIN_NOTIFICATION_TIME + 2, "eq");     // == watermark
        Document liveNew = buildDocument(MIN_NOTIFICATION_TIME + 100, "new"); // > watermark

        givenTail(tail2, tail1);

        ChangeStreamDocument<Document> evOld = insertEvent(liveOld);
        ChangeStreamDocument<Document> evEq = insertEvent(liveEq);
        ChangeStreamDocument<Document> evNew = insertEvent(liveNew);

        Future<List<Event>> future = pool.submit(() ->
                eventService.tailEvents(Mono.just(req))
                        .take(3) // expect: tail1, tail2, liveNew only
                        .timeout(TEST_TIMEOUT)
                        .collectList()
                        .block()
        );

        Consumer<ChangeStreamDocument<Document>> consumer = awaitConsumer();
        consumer.accept(evOld);
        consumer.accept(evEq);
        consumer.accept(evNew);

        List<Event> eventList = future.get(2, TimeUnit.SECONDS);

        then(eventList)
                .hasSize(3)
                .containsExactly(
                        EventParser.parse(tail1),
                        EventParser.parse(tail2),
                        EventParser.parse(liveNew)
                );
    }

    @Test
    void shouldApplyMaxEventsAndStopStream_WhenRepositoryIsInfinite() throws Exception {
        // given
        TailEventsRequest req = buildTailEventsRequest(3); // limit to 3 total

        Document tail1 = buildDocument(MIN_NOTIFICATION_TIME + 1, "tail1");
        Document tail2 = buildDocument(MIN_NOTIFICATION_TIME + 2, "tail2");
        Document inserted1 = buildDocument(MIN_NOTIFICATION_TIME + 100, "a");
        Document inserted2 = buildDocument(MIN_NOTIFICATION_TIME + 200, "b"); // extra one

        givenTail(tail2, tail1);

        ChangeStreamDocument<Document> ev1 = insertEvent(inserted1);
        ChangeStreamDocument<Document> ev2  = insertEvent(inserted2);

        // when
        Future<List<Event>> future = pool.submit(() ->
                eventService.tailEvents(Mono.just(req))
                        .timeout(TEST_TIMEOUT)
                        .collectList()
                        .block()
        );

        Consumer<ChangeStreamDocument<Document>> consumer = awaitConsumer();
        consumer.accept(ev1);
        consumer.accept(ev2);

        List<Event> events = future.get(2, TimeUnit.SECONDS);

        // then (only 3 events total; the 4th should never be observed)
        then(events)
                .hasSize(3)
                .containsExactly(
                        EventParser.parse(tail1),
                        EventParser.parse(tail2),
                        EventParser.parse(inserted1)
                );
    }

    @Test
    void shouldNotLimitEvents_WhenMaxEventsIsZero() throws Exception {
        // given
        TailEventsRequest req = buildTailEventsRequest(0); // no limiting in service

        Document tail1 = buildDocument(MIN_NOTIFICATION_TIME + 1, "tail1");
        Document tail2 = buildDocument(MIN_NOTIFICATION_TIME + 2, "tail2");
        Document inserted1 = buildDocument(MIN_NOTIFICATION_TIME + 100, "a");
        Document inserted2 = buildDocument(MIN_NOTIFICATION_TIME + 200, "b");

        givenTail(tail2, tail1);

        ChangeStreamDocument<Document> ev1 = insertEvent(inserted1);
        ChangeStreamDocument<Document> ev2  = insertEvent(inserted2);

        // when
        Future<List<Event>> future = pool.submit(() ->
                eventService.tailEvents(Mono.just(req))
                        .take(4) // test-level stop
                        .timeout(TEST_TIMEOUT)
                        .collectList()
                        .block()
        );

        Consumer<ChangeStreamDocument<Document>> consumer = awaitConsumer();
        consumer.accept(ev1);
        consumer.accept(ev2);

        List<Event> events = future.get(2, TimeUnit.SECONDS);

        // then
        then(events)
                .hasSize(4)
                .containsExactly(
                        EventParser.parse(tail1),
                        EventParser.parse(tail2),
                        EventParser.parse(inserted1),
                        EventParser.parse(inserted2)
                );
    }

    private void givenTail(Document... returnedByCursorInOrder) {
        given(mongoCollection.find(Filters.gt(NOTIFICATION_TS.name(), MIN_NOTIFICATION_TIME)))
                .willReturn(findIterable);
        given(findIterable.sort(any(Bson.class))).willReturn(findIterable);
        given(findIterable.limit(anyInt())).willReturn(findIterable);
        given(findIterable.iterator()).willReturn(cursor);

        // hasNext = true N times then false
        Boolean[] hasNext = new Boolean[returnedByCursorInOrder.length + 1];
        for (int i = 0; i < returnedByCursorInOrder.length; i++) hasNext[i] = true;
        hasNext[returnedByCursorInOrder.length] = false;

        given(cursor.hasNext()).willReturn(hasNext[0], Arrays.copyOfRange(hasNext, 1, hasNext.length));
        given(cursor.next()).willReturn(returnedByCursorInOrder[0],
                Arrays.copyOfRange(returnedByCursorInOrder, 1, returnedByCursorInOrder.length));
    }

    private ChangeStreamDocument<Document> insertEvent(Document doc) {
        ChangeStreamDocument<Document> ev = mock(ChangeStreamDocument.class);
        given(ev.getOperationType()).willReturn(OperationType.INSERT);
        given(ev.getFullDocument()).willReturn(doc);
        given(ev.getResumeToken()).willReturn(new BsonDocument());
        return ev;
    }

    private Consumer<ChangeStreamDocument<Document>> awaitConsumer() {
        try {
            return consumerFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("Change stream consumer was not captured", e);
        }
    }

    private TailEventsRequest buildTailEventsRequest(int maxEvents) {
        return TailEventsRequest.newBuilder()
                .setRequestId(RequestId.newBuilder().setTraceId("t").setClientId("c").build())
                .setTailSize(5)
                .setMinNotificationTime(MIN_NOTIFICATION_TIME)
                .setMaxEvents(maxEvents)
                .build();
    }

    private Document buildDocument(long ts, String msg) {
        return new Document("notification_ts", ts).append("message", msg);
    }

}

package ru.vicria.event.service.mongo;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import ru.vicria.event.service.api.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


import static org.assertj.core.api.BDDAssertions.then;

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

    @Test
    void shouldProcessMessages_InsertedStrictlyAfterTheTimestamp() {
        withMongo(mongo -> {
            // given
            var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
            var repository = new EventRepositoryImpl(mongo, "123", listener);
            var timeMillis = System.currentTimeMillis();
            var skip = Event.newBuilder()
                    .setNotificationTime(timeMillis)
                    .setMessage("Skip")
                    .build();

            var show = Event.newBuilder()
                    .setNotificationTime(timeMillis + 1)
                    .setMessage("Show")
                    .build();

            // when
            Flux<Event> flux = repository.listenSince(timeMillis).take(Duration.ofSeconds(2));
            repository.insert(List.of(skip, show));
            var list = flux.collectList().block(Duration.ofSeconds(3));

            // then
            then(list).extracting(Event::getMessage).doesNotContain("Skip");
        });
    }

    @Test
    void backpressureBufferOverflow_ShouldEitherErrorOrDropAfterBufferLimit() {
        withMongo(mongo -> {
            // given
            var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
            var repository = new EventRepositoryImpl(mongo, "123", listener);

            long t0 = System.currentTimeMillis();

            int n = 1200; // > 1024 buffer
            var events = new ArrayList<Event>(n);
            for (int i = 0; i < n; i++) {
                events.add(Event.newBuilder()
                        .setNotificationTime(t0 + 1 + i)
                        .setMessage("M" + i)
                        .build());
            }

            // when
            // slow down consumption so sink buffers
            Flux<Event> flux = repository.listenSince(t0)
                    .delayElements(Duration.ofMillis(5))
                    .take(n);

            repository.insert(events);

            List<Event> list = null;
            Throwable error = null;
            try {
                list = flux.collectList().block(Duration.ofSeconds(10));
            } catch (Throwable t) {
                error = t;
            }

            // then
            // Depending on sink behavior, either:
            // - error != null (overflow triggered FAIL_FAST)
            // - or list size < n (some signals dropped / listener died)
            then(error != null || (list != null && list.size() < n)).isTrue();
        });
    }

    @Nested
    @Disabled
    class ImplementationDifferencesObservation {

        /**
         * This test shows that with concat implementation in listenSince there might be an event missed
         * for concat(queryFlux, liveFlux), if an insert happens while queryFlux is still emitting,
         * but before the subscription switches to liveFlux (unless your listener buffers/replays)
         *
         * This test shouldn't be enabled since it's flakiness and exists just for observation purposes.
         */
        @Test
        void shouldIntroducesGapWithConcatInsteadOfMerge_WithInsertDuringInitialQueryMissed() {
            withMongo(mongo -> {
                // given
                var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
                var repository = new EventRepositoryImpl(mongo, "123", listener);

                long t0 = System.currentTimeMillis();

                // create a lot of existing events > t0 so that query phase takes time
                int existing = 5_000;
                var bulk = new ArrayList<Event>(existing);
                for (int i = 0; i < existing; i++) {
                    bulk.add(Event.newBuilder()
                            .setNotificationTime(t0 + 10 + i)
                            .setMessage("E" + i)
                            .build());
                }
                repository.insert(bulk);

                var gapEvent = Event.newBuilder()
                        .setNotificationTime(t0 + 9_999_999)
                        .setMessage("GAP")
                        .build();

                // when
                Flux<Event> flux = repository.listenSince(t0)
                        .take(existing + 1); // try to capture all existing + the gap if it appears

                // insert "GAP" shortly after subscription, while query is presumably still flowing
                Executors.newSingleThreadScheduledExecutor()
                        .schedule(() -> repository.insert(List.of(gapEvent)), 10, TimeUnit.MILLISECONDS);

                var list = flux.collectList().block(Duration.ofSeconds(5));

                // then
                // With concat-based impl and multicast sink, GAP is likely to be missed.
                // With merge, or with replay sink, GAP is likely to appear.
                then(list).isNotNull();
                boolean gapSeen = list.stream().anyMatch(e -> e.getMessage().equals("GAP"));
                then(gapSeen).isFalse();
            });
        }

        @Test
        void shouldShowDifferentInterleavingBetweenMergeAndConcat() {
            withMongo(mongo -> {
                var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
                var repository = new EventRepositoryImpl(mongo, "123", listener);

                long t0 = System.currentTimeMillis();

                // Pre-insert a "query batch" of many events
                int pre = 1000;
                var preEvents = new java.util.ArrayList<Event>(pre);
                for (int i = 1; i <= pre; i++) {
                    preEvents.add(Event.newBuilder()
                            .setNotificationTime(t0 + i)
                            .setMessage("Q" + i)
                            .build());
                }
                repository.insert(preEvents);

                // Live events - clearly distinguishable by message prefix "L"
                var live = List.of(
                        Event.newBuilder().setNotificationTime(t0 + 10_000).setMessage("L1").build(),
                        Event.newBuilder().setNotificationTime(t0 + 10_001).setMessage("L2").build(),
                        Event.newBuilder().setNotificationTime(t0 + 10_002).setMessage("L3").build()
                );

                // Subscribe and slow down downstream so query cannot instantly drain.
                Flux<String> flux = repository.listenSince(t0)
                        .map(Event::getMessage)
                        .delayElements(Duration.ofMillis(2))
                        .take(pre);

                // Insert live events shortly after subscription
                Executors.newSingleThreadScheduledExecutor()
                        .schedule(() -> repository.insert(live), 10, TimeUnit.MILLISECONDS);

                var list = flux.collectList().block(Duration.ofSeconds(5));
                then(list).isNotNull();

                // Observations:
                // - concat impl: first elements should be from query batch ("Q..."), live ("L...") appears only after query completes.
                // - merge impl: it's possible (often likely) that "L1/L2/L3" show up before query finishes => appear within first takeN.
                boolean liveAppearedEarly = list.stream().anyMatch(m -> m.startsWith("L"));

                // This assertion is implementation-dependent:
                // For MERGE version you typically expect: true
                // For CONCAT version you typically expect: false (unless query finishes fast)
                System.out.println("First messages: " + list);
                System.out.println("liveAppearedEarly=" + liveAppearedEarly);
                then(liveAppearedEarly).isTrue();
            });
        }

        @Test
        void shouldProduceDuplicatesWithMerge_WhenNoDistinctInFluxChain_WhenSameEventFromQueryAndChangeStream() {
            withMongo(mongo -> {
                // given
                var listener = new CollectionInsertListenerImpl(Executors.newSingleThreadScheduledExecutor());
                var repository = new EventRepositoryImpl(mongo, "123", listener);

                long t0 = System.currentTimeMillis();

                var show = Event.newBuilder()
                        .setNotificationTime(t0 + 1)
                        .setMessage("Show")
                        .build();

                // when
                // subscribe asynchronously to increase overlap between query and change stream
                Flux<Event> flux = repository.listenSince(t0)
                        .subscribeOn(Schedulers.boundedElastic())
                        .filter(e -> e.getMessage().equals("Show"))
                        .take(2); // if duplicate happens, we'll capture both

                repository.insert(List.of(show));
                var list = flux.collectList().block(Duration.ofSeconds(3));

                // then
                // If your current merge causes duplication, you'll get ["Show", "Show"].
                // If not duplicated (timing), you may get ["Show"].
                then(list).isNotNull();
                then(list).extracting(Event::getMessage)
                        .hasSize(2)
                        .allMatch("Show"::equals);
            });
        }

    }

}

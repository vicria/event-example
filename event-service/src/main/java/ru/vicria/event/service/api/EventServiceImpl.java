package ru.vicria.event.service.api;

import io.grpc.BindableService;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.vicria.event.service.mongo.EventRepositoryImpl;
import ru.vicria.event.service.utils.ReactiveRequestLogger;

public class EventServiceImpl extends ReactorEventServiceGrpc.EventServiceImplBase implements BindableService {

    private final EventRepositoryImpl repository;
    private final Logger logger = LoggerFactory.getLogger(EventServiceImpl.class);

    public EventServiceImpl(EventRepositoryImpl repository) {
        this.repository = repository;
    }

    @Override
    public Flux<Event> listenEvent(Mono<ListenEventRequest> request) {
        return ReactiveRequestLogger.with(logger)
                .forRequest("ListenEvent", request, ListenEventRequest::getRequestId)
                .produce(req -> repository.listenSince(req.getStartingFrom()))
                .doOnComplete(() -> logger.info("Event sent to consumer {}", request));
    }

    @Override
    public Mono<EventAnalysisResult> analyzeEvents(Flux<AnalyticsEventRequest> requests) {
        return requests
                .reduce(new Acc(null, 0L, 0L, 0L), Acc::add)
                .map(acc -> {
                    if (acc.requestId() == null) {
                        throw Status.INVALID_ARGUMENT
                                .withDescription("At least one AnalyticsEventRequest is required")
                                .asRuntimeException();
                    }

                    return EventAnalysisResult.newBuilder()
                            .setRequestId(acc.requestId())
                            .setTotalEvents(acc.totalEvents())
                            .setLatestNotificationTime(acc.latestNotificationTime())
                            .setTotalMessageChars(acc.totalMessageChars())
                            .build();
                });
    }

}

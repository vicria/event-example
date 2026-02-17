package ru.vicria.event.service.api;

import io.grpc.BindableService;
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
                .forRequest("ListenEvent", ListenEventRequest::getRequestId)
                .produce(request, req -> repository.listenSince(req.getStartingFrom()))
                .doOnComplete(() -> logger.info("Event sent to consumer {}", request));
    }

    @Override
    public Mono<EventAnalysisResult> analyzeEvents(Flux<AnalyticsEvent> requests) {
        return ReactiveRequestLogger.with(logger)
                .forRequest("analyzeEvents", AnalyticsEvent::getRequestId)
                .produceMonoReduced(
                        requests,
                        flux -> flux.flatMap(repository::saveOne),
                        requestId -> EventAnalysisResult.newBuilder().setRequestId(requestId),
                        EventServiceImpl::add,
                        EventAnalysisResult.Builder::build
                );
    }

    @Override
    public Flux<Event> tailEvents(Mono<TailEventsRequest> request) {
        return ReactiveRequestLogger.with(logger)
                .forRequest("tailEvents", TailEventsRequest::getRequestId)
                .produce(request, repository::tailThenListen)
                .doOnComplete(() -> logger.info("Tail Events completed for request {}", request));
    }

    private static EventAnalysisResult.Builder add(EventAnalysisResult.Builder builder, Event event) {
        long totalEvents = builder.getTotalEvents() + 1;
        long latestTs = Math.max(builder.getLatestNotificationTime(), event.getNotificationTime());
        long totalChars = builder.getTotalMessageChars() + event.getMessage().length();

        return builder
                .setTotalEvents(totalEvents)
                .setLatestNotificationTime(latestTs)
                .setTotalMessageChars(totalChars);
    }

}

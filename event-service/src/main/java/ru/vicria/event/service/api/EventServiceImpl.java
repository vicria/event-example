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
                .forRequest("ListenEvent", request, ListenEventRequest::getRequestId)
                .produce(req -> repository.listenSince(req.getStartingFrom()))
                .doOnComplete(() -> logger.info("Event sent to consumer {}", request));
    }
}

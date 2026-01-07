package ru.vicria.event.service.utils;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.vicria.event.service.api.RequestId;

import java.util.function.Function;

public class ReactiveRequestLogger<T> {

    private final RequestLogger logger;
    private final String name;
    private final Mono<T> request;
    private final Function<T, RequestId> requestIdExtractor;

    private ReactiveRequestLogger(RequestLogger logger, String name, Mono<T> request, Function<T, RequestId> requestIdExtractor) {
        this.logger = logger;
        this.name = name;
        this.request = request;
        this.requestIdExtractor = requestIdExtractor;
    }

    public static ReactiveRequestLoggerBuilder with(Logger logger) {
        return new ReactiveRequestLoggerBuilder(new Slf4jRequestLogger(logger));
    }

    @SuppressWarnings("unchecked")
    public <R> Flux<R> produce(Function<T, Flux<R>> mapper) {
        return request.flatMapMany(req -> {
            var requestId = requestIdExtractor.apply(req);
            logger.start(name, requestId);
            return Mono.just(req)
                    .flatMapMany(mapper)
                    .doOnNext(response -> logger.process(name, requestId, response))
                    .doOnComplete(() -> logger.end(name, requestId))
                    .doOnCancel(()-> logger.cancel(name, requestId))
                    .doOnError(error -> logger.error(name, requestId, error));
        });
    }

    public static class ReactiveRequestLoggerBuilder {
        private final RequestLogger logger;

        ReactiveRequestLoggerBuilder(RequestLogger logger) {
            this.logger = logger;
        }

        public <T> ReactiveRequestLogger<T> forRequest(String name, Mono<T> request, Function<T, RequestId> requestIdExtractor) {
            return new ReactiveRequestLogger<>(logger, name, request, requestIdExtractor);
        }
    }
}

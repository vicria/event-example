package ru.vicria.event.service.utils;

import io.grpc.Status;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.vicria.event.service.api.RequestId;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ReactiveRequestLogger<T> {

    private final RequestLogger logger;
    private final String name;
    private final Function<T, RequestId> requestIdExtractor;

    private ReactiveRequestLogger(RequestLogger logger, String name, Function<T, RequestId> requestIdExtractor) {
        this.logger = logger;
        this.name = name;
        this.requestIdExtractor = requestIdExtractor;
    }

    public static ReactiveRequestLoggerBuilder with(Logger logger) {
        return new ReactiveRequestLoggerBuilder(new Slf4jRequestLogger(logger));
    }

    public <R> Flux<R> produce(Mono<T> request, Function<T, Flux<R>> mapper) {
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

    public <I, A, R> Mono<R> produceMonoReduced(
            Flux<T> requests,
            Function<Flux<T>, Flux<I>> mapper,
            Function<RequestId, A> seedFactory,
            BiFunction<A, I, A> reduce,
            Function<A, R> finish) {
        return requests.switchOnFirst((firstSignal, flux) -> {
            if (!firstSignal.hasValue()) {
                return Mono.error(Status.INVALID_ARGUMENT
                        .withDescription("At least one AnalyticsEventRequest is required")
                        .asRuntimeException());
            }

            T first = firstSignal.get();
            RequestId requestId = requestIdExtractor.apply(first);

            logger.start(name, requestId);

            Flux<T> validated = flux.doOnNext(req -> {
                RequestId rid = requestIdExtractor.apply(req);
                if (!requestId.equals(rid)) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("All request messages must have the same request_id")
                            .asRuntimeException();
                }
            });

            return mapper.apply(validated)
                    .doOnNext(item -> logger.process(name, requestId, item))
                    .reduce(seedFactory.apply(requestId), reduce)
                    .map(finish)
                    .doOnSuccess(r -> logger.end(name, requestId))
                    .doOnCancel(() -> logger.cancel(name, requestId))
                    .doOnError(err -> logger.error(name, requestId, err))
                    .flux();
        }).single();
    }

    public static class ReactiveRequestLoggerBuilder {
        private final RequestLogger logger;

        ReactiveRequestLoggerBuilder(RequestLogger logger) {
            this.logger = logger;
        }

        public <T> ReactiveRequestLogger<T> forRequest(String name, Function<T, RequestId> requestIdExtractor) {
            return new ReactiveRequestLogger<>(logger, name, requestIdExtractor);
        }
    }
}

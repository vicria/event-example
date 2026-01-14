package ru.vicria.event.service.api;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.vicria.event.service.mongo.EventRepositoryImpl;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class EventServiceImplTest {

    @Mock
    private EventRepositoryImpl repository;

    private EventServiceImpl eventService;

    @BeforeEach
    void setUp() {
        eventService = new EventServiceImpl(repository);
    }

    @Nested
    class ListenEvent {

        @Test
        void shouldReturnEventsFromRepository_OnHappyPath() {
            // given
            ListenEventRequest request = ListenEventRequest.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("trace-1").setClientId("client-1").build())
                    .setStartingFrom(100L)
                    .build();

            Event first = Event.newBuilder().setNotificationTime(110L).setMessage("first").build();
            Event second = Event.newBuilder().setNotificationTime(120L).setMessage("second").build();

            given(repository.listenSince(100L)).willReturn(Flux.just(first, second));

            // when
            var result = eventService.listenEvent(Mono.just(request)).collectList().block();

            // then
            then(result).isNotNull();
            then(result).containsExactly(first, second);
            verify(repository).listenSince(100L);
        }

        @Test
        void shouldPropagateRepositoryFailure_WhenListenSinceErrors() {
            // given
            ListenEventRequest request = ListenEventRequest.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("trace-1").setClientId("client-1").build())
                    .setStartingFrom(200L)
                    .build();

            given(repository.listenSince(200L))
                    .willReturn(Flux.error(new IllegalStateException("boom")));

            // when
            // then
            thenThrownBy(() -> eventService.listenEvent(Mono.just(request)).collectList().block())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("boom");

            verify(repository).listenSince(200L);
        }
    }

    @Nested
    class AnalyzeEvents {

        @Test
        void shouldAggregatesStreamOfAnalyzeEventsIntoResult_OnHappyPath() {
            // given
            RequestId requestId = RequestId.newBuilder()
                    .setTraceId("trace-1")
                    .setClientId("client-1")
                    .build();

            AnalyticsEventRequest first = AnalyticsEventRequest.newBuilder()
                    .setRequestId(requestId)
                    .setEvent(Event.newBuilder().setNotificationTime(10L).setMessage("hi").build())
                    .build();

            AnalyticsEventRequest second = AnalyticsEventRequest.newBuilder()
                    .setRequestId(requestId)
                    .setEvent(Event.newBuilder().setNotificationTime(40L).setMessage("world").build())
                    .build();

            // when
            EventAnalysisResult result = eventService.analyzeEvents(Flux.just(first, second)).block();

            // then
            then(result).isNotNull();
            then(result.getRequestId()).isEqualTo(requestId);
            then(result.getTotalEvents()).isEqualTo(2L);
            then(result.getLatestNotificationTime()).isEqualTo(40L);
            then(result.getTotalMessageChars()).isEqualTo(7L);
            verifyNoInteractions(repository);
        }

        @Test
        void shouldFailWithStatusRuntimeException_WhenStreamIsEmpty() {
            // given

            // when
            // then
            thenThrownBy(() -> eventService.analyzeEvents(Flux.empty()).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("At least one AnalyticsEventRequest is required")
                    .satisfies(ex -> then(((StatusRuntimeException) ex).getStatus().getCode())
                            .isEqualTo(Status.INVALID_ARGUMENT.getCode()));

            verifyNoInteractions(repository);
        }

        @Test
        void shouldFailWithStatusRuntimeException_WhenRequestIdsDiffer() {
            // given
            AnalyticsEventRequest first = AnalyticsEventRequest.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("t1").setClientId("c1").build())
                    .setEvent(Event.newBuilder().setNotificationTime(10L).setMessage("hi").build())
                    .build();

            AnalyticsEventRequest second = AnalyticsEventRequest.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("t2").setClientId("c1").build())
                    .setEvent(Event.newBuilder().setNotificationTime(20L).setMessage("ho").build())
                    .build();


            // when
            // then
            thenThrownBy(() -> eventService.analyzeEvents(Flux.just(first, second)).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("All AnalyticsEventRequest messages must have the same request_id")
                    .satisfies(ex -> then(((StatusRuntimeException) ex).getStatus().getCode())
                            .isEqualTo(Status.INVALID_ARGUMENT.getCode()));

            verifyNoInteractions(repository);
        }
    }
}

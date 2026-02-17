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
import static org.mockito.Mockito.verifyNoMoreInteractions;

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

            AnalyticsEvent first = AnalyticsEvent.newBuilder()
                    .setRequestId(requestId)
                    .setEvent(Event.newBuilder().setNotificationTime(10L).setMessage("hi").build())
                    .build();

            AnalyticsEvent second = AnalyticsEvent.newBuilder()
                    .setRequestId(requestId)
                    .setEvent(Event.newBuilder().setNotificationTime(40L).setMessage("world").build())
                    .build();

            given(repository.saveOne(first)).willReturn(Mono.just(first.getEvent()));
            given(repository.saveOne(second)).willReturn(Mono.just(second.getEvent()));

            // when
            EventAnalysisResult result = eventService.analyzeEvents(Flux.just(first, second)).block();

            // then
            then(result).isNotNull();
            then(result.getRequestId()).isEqualTo(requestId);
            then(result.getTotalEvents()).isEqualTo(2L);
            then(result.getLatestNotificationTime()).isEqualTo(40L);
            then(result.getTotalMessageChars()).isEqualTo(7L);
        }

        @Test
        void shouldFailWithStatusRuntimeException_WhenStreamIsEmpty() {
            // given

            // when
            // then
            thenThrownBy(() -> eventService.analyzeEvents(Flux.empty()).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(ex -> {
                        StatusRuntimeException sre = (StatusRuntimeException) ex;
                        then(sre.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
                        then(sre.getStatus().getDescription()).contains("At least one AnalyticsEventRequest is required");
                    });

            verifyNoInteractions(repository);
        }

        @Test
        void shouldFailWithStatusRuntimeException_WhenRequestIdsDiffer() {
            // given
            AnalyticsEvent first = AnalyticsEvent.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("t1").setClientId("c1").build())
                    .setEvent(Event.newBuilder().setNotificationTime(10L).setMessage("hi").build())
                    .build();

            AnalyticsEvent second = AnalyticsEvent.newBuilder()
                    .setRequestId(RequestId.newBuilder().setTraceId("t2").setClientId("c1").build())
                    .setEvent(Event.newBuilder().setNotificationTime(20L).setMessage("ho").build())
                    .build();

            given(repository.saveOne(first)).willReturn(Mono.just(first.getEvent()));

            // when
            // then
            thenThrownBy(() -> eventService.analyzeEvents(Flux.just(first, second)).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(ex -> {
                        StatusRuntimeException sre = (StatusRuntimeException) ex;
                        then(sre.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
                        then(sre.getStatus().getDescription()).contains("All request messages must have the same request_id");
                    });

            verify(repository).saveOne(first);
            verifyNoMoreInteractions(repository);
        }
    }
}

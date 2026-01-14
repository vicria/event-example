package ru.vicria.event.service.api;

import io.grpc.Status;

import java.util.Optional;

record Acc(RequestId requestId, long totalEvents, long latestNotificationTime, long totalMessageChars) {
    Acc add(AnalyticsEventRequest req) {
        enforceSameRequestIdForMessages(req);

        Event e = Optional.of(req.getEvent())
                .orElseThrow(NoSuchFieldError::new);

        long notificationTime = e.getNotificationTime();
        long messageChars = e.getMessage().length();

        return new Acc(
                requestId == null ? req.getRequestId() : requestId,
                totalEvents + 1,
                Math.max(latestNotificationTime, notificationTime),
                totalMessageChars + messageChars
        );
    }

    private void enforceSameRequestIdForMessages(AnalyticsEventRequest req) {
        if (requestId != null && !requestId.equals(req.getRequestId())) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("All AnalyticsEventRequest messages must have the same request_id")
                    .asRuntimeException();
        }
    }
}

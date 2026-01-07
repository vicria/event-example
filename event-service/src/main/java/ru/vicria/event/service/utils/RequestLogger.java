package ru.vicria.event.service.utils;

import ru.vicria.event.service.api.RequestId;

public interface RequestLogger {
    void start(String name, RequestId requestId);

    void end(String name, RequestId requestId);

    void cancel(String name, RequestId requestId);

    void error(String name, RequestId requestId, Throwable error);

    void process(String name, RequestId requestId, Object response);
}

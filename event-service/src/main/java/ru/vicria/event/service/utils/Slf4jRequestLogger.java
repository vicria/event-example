package ru.vicria.event.service.utils;

import org.slf4j.Logger;
import ru.vicria.event.service.api.RequestId;

public class Slf4jRequestLogger implements RequestLogger {
    private final Logger logger;

    public Slf4jRequestLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void start(String name, RequestId requestId) {
        logger.info("{} started by {}", name, requestId);
    }

    @Override
    public void end(String name, RequestId requestId) {
        logger.info("{} completed by {}", name, requestId);
    }

    @Override
    public void cancel(String name, RequestId requestId) {
        logger.info("{} canceled by {}", name, requestId);
    }

    @Override
    public void error(String name, RequestId requestId, Throwable error) {
        logger.error("On {} by {}", name, requestId, error);
    }

    @Override
    public void process(String name, RequestId requestId, Object response) {
        logger.error("Processing {} by {} with response {}", name, requestId, response);
    }
}

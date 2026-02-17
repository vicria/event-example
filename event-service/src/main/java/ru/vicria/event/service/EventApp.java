package ru.vicria.event.service;

import io.grpc.ServerBuilder;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ru.vicria.event.service.api.EventServiceImpl;
import ru.vicria.event.service.config.EventRepositoryConfiguration;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class EventApp {

    public static final String CLIENT_ID = "event-service";
    public static final Logger logger = LoggerFactory.getLogger(EventApp.class);

    public static void main(String[] args) {
        logger.info("Starting {} ...", CLIENT_ID);
        try {
            setDefaultExceptionHandler();
            final var repositoryConfiguration = new EventRepositoryConfiguration();
            final var repository = repositoryConfiguration.getRepository();

            repository.listenSince(System.currentTimeMillis())
                    .subscribeOn(UncaughtExceptionSchedulers.newSingle("event-listener-monitor", true))
                    .doOnNext(e -> logger.info("Process a message {}", e))
                    .doOnError(e -> logger.error("Error on listener ", e))
                    .doOnTerminate(() -> {
                        logger.warn("Listener has been finished, shutting down...");
                        System.exit(1);
                    }).subscribe();

            EventServiceImpl eventService = new EventServiceImpl(repository);

            var grpcServer = ServerBuilder
                    .forPort(6565)
                    .addService(eventService)
                    .build()
                    .start();

            long currentTs = Instant.now().toEpochMilli();

            logger.info("gRPC server started on port 6565 at " + currentTs);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down gRPC server...");
                grpcServer.shutdown();
            }));

            waitTermination();
        } catch (Exception e) {
            logger.error("Failed to start {}.", CLIENT_ID, e);
            System.exit(1);
        }
    }

    private static void setDefaultExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t,e) ->{
            logger.error("Unhandled exception in Thread {}.", t, e);
            System.exit(1);
        });
    }

    private static void waitTermination(){
        while (!Thread.interrupted()){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                logger.info("Terminate app {} ...", CLIENT_ID);
                Thread.currentThread().interrupt();
            }
        }
    }
}
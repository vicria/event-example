package ru.vicria.event.service.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MongoDBContainer;

import java.util.function.Consumer;

public abstract class BaseMongoTest {

    private static final MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @BeforeAll
    static void setUp() {
        mongo.start();
    }

    @AfterAll
    static void shutdown() {
        mongo.stop();
    }

    protected void withMongo(Consumer<MongoDatabase> test) {
        try (var mongoClient = MongoClients.create(mongo.getConnectionString());
             var testDatabase = new TestDatabase(mongoClient)) {
            test.accept(testDatabase.get());
        }
    }

    static class TestDatabase implements AutoCloseable {

        private final MongoDatabase testDatabase;

        public TestDatabase(MongoClient mongoClient) {
            testDatabase = mongoClient.getDatabase(String.format("%s_%s", this.getClass().getSimpleName(), System.currentTimeMillis()));
        }

        public MongoDatabase get() {
            return testDatabase;
        }

        @Override
        public void close() {
            testDatabase.drop();
        }
    }
}

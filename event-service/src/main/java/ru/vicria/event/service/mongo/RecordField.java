package ru.vicria.event.service.mongo;

import org.bson.Document;

import java.util.Optional;

public class RecordField<T> {

    private final String name;

    public RecordField(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public T from(Document doc) {
        return (T) doc.get(name);
    }

    public Optional<T> find(Document doc) {
        return Optional.ofNullable(from(doc));
    }
}

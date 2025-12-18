package ru.vicria.event.service.mongo;

public final class EventField {

    public static final RecordField<String> MESSAGE = new RecordField<>("message");
    public static final RecordField<Long> NOTIFICATION_TS = new RecordField<>("notification_ts");

    private EventField() {
    }
}

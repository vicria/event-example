package ru.vicria.event.service.mongo;

import org.bson.Document;
import ru.vicria.event.service.api.Event;

public final class EventParser {
    private EventParser() {
    }

    public static Event parse(Document doc) {
        return Event.newBuilder()
                .setNotificationTime(EventField.NOTIFICATION_TS.from(doc))
                .setMessage(EventField.MESSAGE.from(doc))
                .build();
    }

    public static Document toDocument(Event event){
        return new Document()
                .append(EventField.MESSAGE.name(), event.getMessage())
                .append(EventField.NOTIFICATION_TS.name(), event.getNotificationTime());
    }
}

package com.helium.ingestor.core;

public interface EventNotifier {
    enum EventType {
        CAMERA_PROCESS_TERMINATED,
        CAMERA_PROCESS_FORCIBLY_KILLED
    }

    void notifyEvent(EventType eventType, String cameraName, String eventTitle, String eventDetails);
}

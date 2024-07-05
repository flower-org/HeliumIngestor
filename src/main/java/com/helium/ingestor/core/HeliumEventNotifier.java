package com.helium.ingestor.core;

import javax.annotation.Nullable;

public interface HeliumEventNotifier {
    void notifyEvent(HeliumEventType eventType, @Nullable String cameraName, String eventTitle, String eventDetails);
}

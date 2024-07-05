package com.helium.ingestor.core;

import static com.helium.ingestor.core.DatedFileHeliumEventNotifier.getEventMessageStr;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class LogHeliumEventNotifier implements HeliumEventNotifier {
    final static Logger LOGGER = LoggerFactory.getLogger(LogHeliumEventNotifier.class);

    @Override
    public void notifyEvent(HeliumEventType eventType, @Nullable String cameraName, String eventTitle, @Nullable String eventDetails) {
        LOGGER.warn("!!!EVENT!!! {}", getEventMessageStr(eventType, cameraName, eventTitle, eventDetails));
    }
}

package com.helium.ingestor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.helium.ingestor.core.DatedFileHeliumEventNotifier.getEventMessageStr;

@Deprecated
public class LogHeliumEventNotifier implements HeliumEventNotifier {
    final static Logger LOGGER = LoggerFactory.getLogger(LogHeliumEventNotifier.class);

    @Override
    public void notifyEvent(HeliumEventType eventType, @Nullable String cameraName, String eventTitle, String eventDetails) {
        LOGGER.warn("!!!EVENT!!! {}", getEventMessageStr(eventType, cameraName, eventTitle, eventDetails));
    }
}

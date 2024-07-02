package com.helium.ingestor.core;

import com.helium.ingestor.flows.MainIngestorFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEventNotifier implements EventNotifier{
    final static Logger LOGGER = LoggerFactory.getLogger(MainIngestorFlow.class);

    @Override
    public void notifyEvent(EventType eventType, String cameraName, String eventTitle, String eventDetails) {
        LOGGER.warn(String.format("!!!EVENT!!! [%s] from cam [%s]. [%s]:\n\t%s",
                eventType, cameraName, eventTitle, eventDetails)
        );
    }
}

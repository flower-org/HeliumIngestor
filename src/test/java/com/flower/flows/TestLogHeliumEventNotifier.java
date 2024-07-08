package com.flower.flows;

import static com.helium.ingestor.core.DatedFileHeliumEventNotifier.getEventMessageStr;

import javax.annotation.Nullable;

import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogHeliumEventNotifier implements HeliumEventNotifier {
    final static Logger LOGGER = LoggerFactory.getLogger(TestLogHeliumEventNotifier.class);

    @Override
    public void notifyEvent(Long startUnixTimeMs, Long endUnixTimeMs, HeliumEventType eventType, @Nullable String cameraName, String eventTitle, @Nullable String eventDetails) {
        LOGGER.warn("!!!EVENT!!! {}", getEventMessageStr(startUnixTimeMs, endUnixTimeMs, eventType, cameraName, eventTitle, eventDetails));
    }
}

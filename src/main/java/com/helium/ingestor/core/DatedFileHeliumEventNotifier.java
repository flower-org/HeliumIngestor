package com.helium.ingestor.core;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatedFileHeliumEventNotifier implements HeliumEventNotifier {
    final static Logger LOGGER = LoggerFactory.getLogger(DatedFileHeliumEventNotifier.class);
    final static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    final static String HELIUM_EVENTS_FILENAME_SUFFIX = "-helium-events.log";

    protected @Nullable BufferedWriter currentEventFileWriter;
    protected @Nullable String currentEventFileName = null;
    protected final File eventsFolder;

    public DatedFileHeliumEventNotifier(File eventsFolder) {
        this.eventsFolder = eventsFolder;
        if (!eventsFolder.exists()) {
            eventsFolder.mkdirs();
        }
    }

    public static String getEventMessageStr(Long startUnixTimeMillis, Long endUnixTimeMillis, HeliumEventType eventType, @Nullable String cameraName,
                                            String eventTitle, @Nullable String eventDetails) {
        LocalDateTime startEventDateTime = Instant.ofEpochMilli(startUnixTimeMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime endEventDateTime = Instant.ofEpochMilli(endUnixTimeMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();
        String startEventDateTimeStr = startEventDateTime.format(DATE_TIME_FORMATTER);
        String endEventDateTimeStr = endEventDateTime.format(DATE_TIME_FORMATTER);

        if (startEventDateTimeStr.equals(endEventDateTimeStr)) {
            return String.format("Camera [%s]: [%s]. Time: [%s]; [%s]:\n\t%s",
                    cameraName, eventType, startEventDateTimeStr, eventTitle, eventDetails);
        } else {
            return String.format("Camera [%s]: [%s]. From: [%s]-[%s]; [%s]:\n\t%s",
                    cameraName, eventType, startEventDateTimeStr, endEventDateTimeStr, eventTitle, eventDetails);
        }
    }

    protected BufferedWriter getEventFileWriter() throws IOException {
        String newDateStr = LocalDate.now().format(DATE_FORMATTER);
        String newEventFileName = String.format("%s%s", newDateStr, HELIUM_EVENTS_FILENAME_SUFFIX);

        if (!newEventFileName.equals(currentEventFileName)) {
            currentEventFileName = newEventFileName;
            File eventFile = new File(eventsFolder, currentEventFileName);
            if (!eventFile.exists()) {
                eventFile.createNewFile();
            }
            currentEventFileWriter = new BufferedWriter(new FileWriter(eventFile, true));
        }

        return checkNotNull(currentEventFileWriter);
    }

    @Override
    public void notifyEvent(Long startUnixTimeMs, Long endUnixTimeMs, HeliumEventType eventType, @Nullable String cameraName, String eventTitle, @Nullable String eventDetails) {
        //TODO: remove this debug clause
        if (eventType == HeliumEventType.CAMERA_PROCESS_STARTED || eventType == HeliumEventType.CAMERA_PROCESS_TERMINATED) { return; }

        String eventMessage = getEventMessageStr(startUnixTimeMs, endUnixTimeMs, eventType, cameraName, eventTitle, eventDetails);
        LOGGER.warn("!!!EVENT!!! {}", eventMessage);
        try {
            String timestamp = LocalDateTime.now().format(DATE_TIME_FORMATTER);
            BufferedWriter fileWriter = getEventFileWriter();
            fileWriter.write(String.format("%s %s%n", timestamp, eventMessage));
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

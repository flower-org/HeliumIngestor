package com.helium.ingestor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkNotNull;

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

    public static String getEventMessageStr(HeliumEventType eventType, @Nullable String cameraName,
                                            String eventTitle, @Nullable String eventDetails) {
        return String.format("Camera [%s]: [%s]. [%s]:\n\t%s",
                cameraName, eventType, eventTitle, eventDetails);
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
    public void notifyEvent(HeliumEventType eventType, @Nullable String cameraName, String eventTitle, @Nullable String eventDetails) {
        String eventMessage = getEventMessageStr(eventType, cameraName, eventTitle, eventDetails);
        LOGGER.warn("!!!EVENT!!! {}", getEventMessageStr(eventType, cameraName, eventTitle, eventDetails));
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

package com.helium.ingestor.core;

import javax.annotation.Nullable;

public interface HeliumEventNotifier {
  default void notifyEvent(
      String eventReporter,
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails) {
    long currentTime = System.currentTimeMillis();
    notifyEvent(currentTime, eventReporter, currentTime, eventType, cameraName, eventTitle, eventDetails);
  }

  default void notifyEvent(
      Long reportTimeMs,
      String eventReporter,
      Long eventTimeMs,
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails) {
    notifyEvent(reportTimeMs, eventReporter, eventTimeMs, eventTimeMs, eventType, cameraName, eventTitle, eventDetails);
  }

  void notifyEvent(
      Long eventReportTime,
      String eventReporter,
      Long startUnixTimeMs,
      Long endUnixTimeMs,
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails);
}

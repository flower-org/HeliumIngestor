package com.helium.ingestor.core;

import javax.annotation.Nullable;

public interface HeliumEventNotifier {
  default void notifyEvent(
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails) {
    notifyEvent(System.currentTimeMillis(), eventType, cameraName, eventTitle, eventDetails);
  }

  default void notifyEvent(
      Long unixTimeMs,
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails) {
    notifyEvent(unixTimeMs, unixTimeMs, eventType, cameraName, eventTitle, eventDetails);
  }

  void notifyEvent(
      Long startUnixTimeMs,
      Long endUnixTimeMs,
      HeliumEventType eventType,
      @Nullable String cameraName,
      String eventTitle,
      @Nullable String eventDetails);
}

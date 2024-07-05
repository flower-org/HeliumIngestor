package com.helium.ingestor.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableHeliumEvent.class)
@JsonDeserialize(as = ImmutableHeliumEvent.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
interface HeliumEvent {
  HeliumEventType eventType();

  String cameraName();

  String eventTitle();

  String eventDetails();
}

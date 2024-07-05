package com.helium.ingestor.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableConfig.class)
@JsonDeserialize(as = ImmutableConfig.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface Config {
    /** RTSP camera */
    @Value.Immutable
    @JsonSerialize(as = ImmutableCamera.class)
    @JsonDeserialize(as = ImmutableCamera.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface Camera {
        /** RTSP creds */
        @Value.Immutable
        @JsonSerialize(as = ImmutableCredentials.class)
        @JsonDeserialize(as = ImmutableCredentials.class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        interface Credentials {
            @JsonProperty
            String username();
            @JsonProperty
            String password();
        }

        @JsonProperty
        String name();
        @JsonProperty
        String hostname();
        @Value.Default
        @JsonProperty
        default CameraType type() { return CameraType.DEFAULT; }

        @JsonProperty
        Credentials credentials();
    }

    @JsonProperty
    String videoFeedFolder();
    @JsonProperty
    List<Camera> cameras();

    /** ffmpeg's -timeout parameter (socket timeout in microseconds)
     * Default 1 second (1000000 us)*/
    @Value.Default
    @JsonProperty
    default Long socketTimeout_us() { return 1000000L; }
}

//nohup ffmpeg -i "rtsp://username:password@192.168.1.18/Preview_01_main" -c copy -map 0 -segment_time 00:00:01 -reset_timestamps 1 -strftime 1 -f segment 6/video_%Y-%m-%d_%H_%M_%S.mp4

//"nohup ffmpeg -i \"rtsp://username:password@192.168.1.18/Preview_01_main\" -c copy -map 0 -segment_time 00:00:01 -reset_timestamps 1 -strftime 1 -f segment 6/video_%Y-%m-%d_%H_%M_%S.mp4"

/*
    /sbin/vbetool dpms off
    read ans
    /sbin/vbetool dpms on
    vlock
*/

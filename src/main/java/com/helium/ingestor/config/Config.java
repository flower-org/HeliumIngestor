package com.helium.ingestor.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;

import javax.annotation.Nullable;

@Value.Immutable
@JsonSerialize(as = ImmutableConfig.class)
@JsonDeserialize(as = ImmutableConfig.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface Config {
    /** Username / password creds */
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

    interface Camera {
        @JsonProperty
        String name();

        @Value.Default
        @JsonProperty
        default boolean retainChunksForDebug() { return false; }

        @Value.Default
        @JsonProperty
        default boolean hasVideo() { return true; }

        @Value.Default
        @JsonProperty
        default boolean hasAudio() { return true; }
    }

    /** RTSP camera */
    @Value.Immutable
    @JsonSerialize(as = ImmutableRtspCamera.class)
    @JsonDeserialize(as = ImmutableRtspCamera.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface RtspCamera extends Camera {
        @JsonProperty
        String rtspUrl();

        @JsonProperty
        @Nullable
        Credentials credentials();
    }

    /** Camera defined directly by ffmpeg command */
    @Value.Immutable
    @JsonSerialize(as = ImmutableCommandCamera.class)
    @JsonDeserialize(as = ImmutableCommandCamera.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface CommandCamera extends Camera {
        @JsonProperty
        String commandPrefix();
    }

    /** Camera defined directly by ffmpeg command */
    @Value.Immutable
    @JsonSerialize(as = ImmutableRawCommandCamera.class)
    @JsonDeserialize(as = ImmutableRawCommandCamera.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface RawCommandCamera extends Camera {
        @JsonProperty
        String command();
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableVideoFileService.class)
    @JsonDeserialize(as = ImmutableVideoFileService.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface VideoFileService {
        //TODO: ssl parameters

        @JsonProperty
        int port();

        @JsonProperty
        @Nullable
        Credentials credentials();
    }

    @JsonProperty
    @Nullable
    String log4jFolder();

    @JsonProperty
    String videoFeedFolder();

    @JsonProperty
    List<RtspCamera> rtspCameras();

    @JsonProperty
    List<CommandCamera> commandCameras();

    @JsonProperty
    List<RawCommandCamera> rawCommandCameras();

    /** ffmpeg's -timeout parameter (socket timeout in microseconds)
     * Default 1 second (1000000 us)*/
    @Value.Default
    @JsonProperty
    default Long socketTimeout_us() { return 1000000L; }

    @JsonProperty
    @Nullable
    VideoFileService videoFileService();

    /** ffmpeg's -timeout parameter (socket timeout in microseconds)
     * Default 1 second (1000000 us)*/
    @Value.Default
    @JsonProperty
    default boolean debugOutputMergeChunkList() { return false; }
}

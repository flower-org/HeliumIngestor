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

    /** RTSP camera */
    @Value.Immutable
    @JsonSerialize(as = ImmutableCamera.class)
    @JsonDeserialize(as = ImmutableCamera.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface Camera {
        @JsonProperty
        String name();
        @JsonProperty
        String rtspUrl();

        @JsonProperty
        @Nullable
        Credentials credentials();

        @Value.Default
        @JsonProperty
        default boolean retainChunksForDebug() { return false; }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableVideoService.class)
    @JsonDeserialize(as = ImmutableVideoService.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    interface VideoService {
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
    List<Camera> cameras();

    /** ffmpeg's -timeout parameter (socket timeout in microseconds)
     * Default 1 second (1000000 us)*/
    @Value.Default
    @JsonProperty
    default Long socketTimeout_us() { return 1000000L; }

    @JsonProperty
    @Nullable
    VideoService videoService();

    /** ffmpeg's -timeout parameter (socket timeout in microseconds)
     * Default 1 second (1000000 us)*/
    @Value.Default
    @JsonProperty
    default boolean debugOutputMergeChunkList() { return false; }
}

package com.helium.ingestor.config;

import java.net.URI;
import java.net.URISyntaxException;

import static com.helium.ingestor.config.Config.Camera;
import static com.helium.ingestor.config.Config.Credentials;
import static com.helium.ingestor.config.Config.RtspCamera;
import static com.helium.ingestor.config.Config.CommandCamera;

public class FfmpegCommandCreator {
    static final String FFMPEG_RTSP_CMD_PATTERN =
            "ffmpeg -i %s -c copy -map 0" +
                    " -timeout %d -f segment -segment_time 00:00:01 -strftime 1" +
                    " -reset_timestamps 1 %s/%s/video_%%Y-%%m-%%d_%%H_%%M_%%S.mp4";

    static final String FFMPEG_GENERIC_CMD_PATTERN =
                    "%s -timeout %d -f segment -segment_time 00:00:01 -strftime 1" +
                    " -reset_timestamps 1 %s/%s/video_%%Y-%%m-%%d_%%H_%%M_%%S.mp4";

    public static String createFfmpegCommand(Camera camera, String videoFeedFolder, Long socketTimeout_us) throws URISyntaxException {
        if (camera instanceof RtspCamera) {
            RtspCamera rtspCamera = (RtspCamera)camera;

            String rtspUrlNoCreds = rtspCamera.rtspUrl();
            URI url = new URI(rtspUrlNoCreds);
            if (rtspCamera.credentials() != null) {
                //Add RTSP credentials if specified
                Credentials creds = rtspCamera.credentials();
                url = new URI(url.getScheme(),
                        String.format("%s:%s", creds.username(), creds.password()),
                        url.getHost(),
                        url.getPort(),
                        url.getPath(),
                        url.getQuery(),
                        url.getFragment());
            }

            return String.format(FFMPEG_RTSP_CMD_PATTERN, url, socketTimeout_us, videoFeedFolder, camera.name());
        } else if (camera instanceof CommandCamera) {
                CommandCamera commandCamera = (CommandCamera)camera;
            return String.format(FFMPEG_GENERIC_CMD_PATTERN, commandCamera.commandPrefix(), socketTimeout_us, videoFeedFolder, camera.name());
        } else {
            throw new IllegalArgumentException("Unknown camera type:" + camera.getClass().getCanonicalName());
        }
    }
}

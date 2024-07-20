package com.helium.ingestor.flows;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.step.FlowRepo;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.FlowRepoPrm;
import com.flower.conf.Transition;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.core.HeliumEventNotifier;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FlowType(firstStep = "LOAD_CAMERAS_FROM_CONFIG")
public class MainIngestorFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(MainIngestorFlow.class);

    static final String FFMPEG_RTSP_CMD_PATTERN =
            "ffmpeg -i %s -c copy -map 0 -timeout %d -segment_time 00:00:01" +
            " -reset_timestamps 1 -strftime 1 -f segment %s/%s/video_%%Y-%%m-%%d_%%H_%%M_%%S.mp4";

    @State final Config config;
    @State final HeliumEventNotifier heliumEventNotifier;
    /** This Map - < CameraName, FfmpegCommand > */
    @State final Map<String, CommandAndSettings> cameraNameToCmdMap;

    public static class CommandAndSettings {
        final String command;
        final boolean debugRetainChunks;
        final boolean cameraHasAudio;
        final boolean cameraHasVideo;

        CommandAndSettings(String command, boolean debugRetainChunks,
                           boolean cameraHasAudio, boolean cameraHasVideo) {
            this.command = command;
            this.cameraHasAudio = cameraHasAudio;
            this.cameraHasVideo = cameraHasVideo;
            this.debugRetainChunks = debugRetainChunks;
        }
    }

    public MainIngestorFlow(Config config, HeliumEventNotifier heliumEventNotifier1) {
        this.config = config;
        this.heliumEventNotifier = heliumEventNotifier1;
        this.cameraNameToCmdMap = new HashMap<>();
    }

    @SimpleStepFunction
    public static Transition LOAD_CAMERAS_FROM_CONFIG(
            @In Config config,
            @In Map<String, CommandAndSettings> cameraNameToCmdMap,
            @FlowRepo FlowRepoPrm repo,
            @StepRef Transition RUN_CHILD_FLOWS) throws URISyntaxException {
        for (Config.Camera camera : config.cameras()) {
            String rtspUrlNoCreds = camera.rtspUrl();
            URI url = new URI(rtspUrlNoCreds);
            if (camera.credentials() != null) {
                //Add RTSP credentials if specified
                Config.Credentials creds = camera.credentials();
                url = new URI(url.getScheme(),
                        String.format("%s:%s", creds.username(), creds.password()),
                        url.getHost(),
                        url.getPort(),
                        url.getPath(),
                        url.getQuery(),
                        url.getFragment());
            }

            String ffmpegRtspCommand = String.format(FFMPEG_RTSP_CMD_PATTERN,
                    url, config.socketTimeout_us(),
                    config.videoFeedFolder(), camera.name());

            cameraNameToCmdMap.put(camera.name(), new CommandAndSettings(ffmpegRtspCommand,
                    camera.retainChunksForDebug(), camera.hasAudio(), camera.hasVideo()));

            //Create camera feed folder if not found
            File cameraFeedFolder = new File(config.videoFeedFolder() + File.separator + camera.name());
            if (!cameraFeedFolder.exists()) {
                if (!cameraFeedFolder.mkdirs()) {
                    throw new RuntimeException(
                            String.format("Can't create camera folder: [%s] [%s]", camera.name(), cameraFeedFolder));
                } else {
                    LOGGER.info("Created camera folder: [{}] [{}]", camera.name(), cameraFeedFolder);
                }
            }
        }

        return RUN_CHILD_FLOWS;
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> RUN_CHILD_FLOWS(
            @In Config config,
            @In Map<String, CommandAndSettings> cameraNameToCmdMap,
            @In HeliumEventNotifier heliumEventNotifier,
            @FlowFactory(flowType = CameraProcessRunnerFlow.class)
                FlowFactoryPrm<CameraProcessRunnerFlow> ffmpegFlowFactory,
            @FlowFactory(flowType = VideoChunkManagerFlow.class)
                FlowFactoryPrm<VideoChunkManagerFlow> videoChunkFlowFactory,
            @StepRef Transition FINALIZE) {
        //1. Camera feed persist flows
        List<FlowFuture<CameraProcessRunnerFlow>> ffmpegFlowFutures = new ArrayList<>();
        for (Map.Entry<String, CommandAndSettings> cameraEntry : cameraNameToCmdMap.entrySet()) {
            String camera = cameraEntry.getKey();
            String ffmpegCommand = cameraEntry.getValue().command;
            CameraProcessRunnerFlow ffmpegFlow = new CameraProcessRunnerFlow(camera, ffmpegCommand, heliumEventNotifier);
            ffmpegFlowFutures.add(ffmpegFlowFactory.runChildFlow(ffmpegFlow));
        }

        //2. Duration determiner / Feed analyzer / Merger / Continuity checker flow
        List<FlowFuture<VideoChunkManagerFlow>> videoChunkManagerFutures = new ArrayList<>();
        for (Map.Entry<String, CommandAndSettings> cameraEntry : cameraNameToCmdMap.entrySet()) {
            String camera = cameraEntry.getKey();
            boolean debugOutputMergeChunkList = config.debugOutputMergeChunkList();
            boolean debugRetainChunks = cameraEntry.getValue().debugRetainChunks;
            boolean cameraHasAudio = cameraEntry.getValue().cameraHasAudio;
            boolean cameraHasVideo = cameraEntry.getValue().cameraHasVideo;
            VideoChunkManagerFlow chunkManagerFlow = new VideoChunkManagerFlow(new File(config.videoFeedFolder(), camera),
                    camera, cameraHasAudio, cameraHasVideo, debugOutputMergeChunkList, debugRetainChunks, heliumEventNotifier);
            videoChunkManagerFutures.add(videoChunkFlowFactory.runChildFlow(chunkManagerFlow));
        }

        //3. Archiver Flow
        //TODO: start child archiver flows

        SettableFuture<Void> combinedFuture = SettableFuture.create();
        Futures.whenAllComplete(ffmpegFlowFutures.stream().map(FlowFuture::getFuture).toList())
            .call(
                () -> combinedFuture.set(null),
                MoreExecutors.directExecutor()
            );

        return Futures.transform(combinedFuture,
                                 void_ -> FINALIZE,
                                 MoreExecutors.directExecutor());
    }

    @SimpleStepFunction
    public static Transition FINALIZE(@Terminal Transition END) {
        //TODO: finalization actions - Log

        return END;
    }
}

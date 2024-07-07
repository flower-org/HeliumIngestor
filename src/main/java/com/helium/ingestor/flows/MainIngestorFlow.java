package com.helium.ingestor.flows;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.Transition;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.helium.ingestor.config.CameraType;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.core.HeliumEventNotifier;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FlowType(firstStep = "LOAD_CAMERAS_FROM_CONFIG")
public class MainIngestorFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(MainIngestorFlow.class);

    static final String DEFAULT_RTSP_URL_PATTERN = "rtsp://%s:%s@%s";
    static final String REOLINK_RTSP_URL_PATTERN = "rtsp://%s:%s@%s/Preview_01_main";

    static final Map<CameraType, String> RTSP_URL_PATTERN_MAP = Map.of(
            CameraType.DEFAULT, DEFAULT_RTSP_URL_PATTERN,
            CameraType.REOLINK, REOLINK_RTSP_URL_PATTERN
    );

    static final String FFMPEG_RTSP_CMD_PATTERN =
            "ffmpeg -i %s -c copy -map 0 -timeout %d -segment_time 00:00:01" +
            " -reset_timestamps 1 -strftime 1 -f segment %s/%s/video_%%Y-%%m-%%d_%%H_%%M_%%S.mp4";

    @State final Config config;
    @State final HeliumEventNotifier heliumEventNotifier;
    /** This Map - < CameraName, FfmpegCommand > */
    @State final Map<String, String> cameraNameToCmdMap;

    public MainIngestorFlow(Config config, HeliumEventNotifier heliumEventNotifier1) {
        this.config = config;
        this.heliumEventNotifier = heliumEventNotifier1;
        this.cameraNameToCmdMap = new HashMap<>();
    }

    @SimpleStepFunction
    public static Transition LOAD_CAMERAS_FROM_CONFIG(
            @In Config config,
            @In Map<String, String> cameraNameToCmdMap,
            @StepRef Transition RUN_CHILD_FLOWS) {
        for (Config.Camera camera : config.cameras()) {
            String rtspUrlPattern = RTSP_URL_PATTERN_MAP.get(camera.type());
            String rtspUrl = String.format(rtspUrlPattern,
                    camera.credentials().username(), camera.credentials().password(), camera.hostname());
            String ffmpegRtspCommand = String.format(FFMPEG_RTSP_CMD_PATTERN,
                    rtspUrl, config.socketTimeout_us(),
                    config.videoFeedFolder(), camera.name());

            cameraNameToCmdMap.put(camera.name(), ffmpegRtspCommand);

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
            @In Map<String, String> cameraNameToCmdMap,
            @In HeliumEventNotifier heliumEventNotifier,
            @FlowFactory(flowType = CameraProcessRunnerFlow.class)
                FlowFactoryPrm<CameraProcessRunnerFlow> ffmpegFlowFactory,
            @FlowFactory(flowType = VideoChunkManagerFlow.class)
                FlowFactoryPrm<VideoChunkManagerFlow> videoChunkFlowFactory,
            @StepRef Transition FINALIZE) {
        //1. Camera feed persist flows
        List<FlowFuture<CameraProcessRunnerFlow>> ffmpegFlowFutures = new ArrayList<>();
        for (Map.Entry<String, String> cameraEntry : cameraNameToCmdMap.entrySet()) {
            String camera = cameraEntry.getKey();
            String ffmpegCommand = cameraEntry.getValue();
            CameraProcessRunnerFlow ffmpegFlow = new CameraProcessRunnerFlow(camera, ffmpegCommand, heliumEventNotifier);
            ffmpegFlowFutures.add(ffmpegFlowFactory.runChildFlow(ffmpegFlow));
        }

        //2. Duration determiner / Feed analyzer / Merger / Continuity checker flow
        List<FlowFuture<VideoChunkManagerFlow>> videoChunkManagerFutures = new ArrayList<>();
        for (Map.Entry<String, String> cameraEntry : cameraNameToCmdMap.entrySet()) {
            String camera = cameraEntry.getKey();
            String ffmpegCommand = cameraEntry.getValue();
            VideoChunkManagerFlow ffmpegFlow = new VideoChunkManagerFlow(new File(config.videoFeedFolder(), camera), camera, heliumEventNotifier);
            videoChunkManagerFutures.add(videoChunkFlowFactory.runChildFlow(ffmpegFlow));
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

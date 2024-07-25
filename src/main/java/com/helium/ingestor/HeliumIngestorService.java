package com.helium.ingestor;

import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.core.DatedFileHeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.*;
import com.helium.ingestor.flows.events.FlowExceptionEvent;
import com.helium.ingestor.flows.events.FlowTerminationEvent;

import java.io.File;
import java.util.concurrent.ExecutionException;

public class HeliumIngestorService {
    public static final String HELIUM_INGESTOR = "HeliumIngestor";

    public static void run(Config config) throws ExecutionException, InterruptedException {
        if ((config.rtspCameras() == null || config.rtspCameras().isEmpty())
            && (config.commandCameras() == null || config.commandCameras().isEmpty())) {
            throw new IllegalArgumentException("Cameras not set");
        }

        HeliumEventNotifier heliumEventNotifier =
                new DatedFileHeliumEventNotifier(new File(new File(config.videoFeedFolder()), "events"));

        String eventTitleStarting = "Starting HELIUM INGESTOR.";
        heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleStarting, eventTitleStarting);

        Flower flower = new Flower();
        flower.registerFlow(MainIngestorFlow.class);
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.registerFlow(VideoChunkManagerFlow.class);
        flower.registerFlow(LoadChunkVideoDurationFlow.class);
        flower.registerFlow(AnalyzeAndMergeChunkRangeFlow.class);
        flower.registerFlow(MergeChunkSubRangeFlow.class);
        flower.registerFlow(LoadVideoDurationFlow.class);
        flower.registerFlow(LoadMediaChannelsFlow.class);
        flower.registerFlow(FormChunkRangesFlow.class);

        flower.registerEventProfile(FlowTerminationEvent.class, true);
        flower.registerEventProfile(FlowExceptionEvent.class, true);

        FlowTerminationEvent.setNOTIFIER(heliumEventNotifier);
        FlowExceptionEvent.setNOTIFIER(heliumEventNotifier);

        flower.initialize();

        FlowExec<MainIngestorFlow> flowExec = flower.getFlowExec(MainIngestorFlow.class);

        MainIngestorFlow testFlow = new MainIngestorFlow(config, heliumEventNotifier);
        FlowFuture<MainIngestorFlow> flowFuture = flowExec.runFlow(testFlow);

        String eventTitleStarted = "HELIUM INGESTOR started.";
        String eventDetailsStarted = String.format("%s Main flow: [%s]", eventTitleStarted, flowFuture.getFlowId());
        heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleStarted, eventDetailsStarted);

        // Wait until main flow is over (not going to happen, probably)
        MainIngestorFlow flow = flowFuture.getFuture().get();

        String eventTitleShuttingDown = "HELIUM INGESTOR shutting down.";
        String eventDetailsShuttingDown = String.format("%s Main flow done: [%s]", eventTitleShuttingDown, flowFuture.getFlowId());
        heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleShuttingDown, eventDetailsShuttingDown);

        flower.shutdownScheduler();

        String eventTitleShutdown = "HELIUM INGESTOR shut down.";
        heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleShutdown, eventTitleShutdown);
    }
}

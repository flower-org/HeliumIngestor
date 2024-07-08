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
    public static void run(Config config) throws ExecutionException, InterruptedException {
        if (config.cameras() == null || config.cameras().isEmpty()) {
            throw new IllegalArgumentException("Cameras not set");
        }

        HeliumEventNotifier heliumEventNotifier =
                new DatedFileHeliumEventNotifier(new File(new File(config.videoFeedFolder()), "events"));

        heliumEventNotifier.notifyEvent(HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                "Starting HELIUM INGESTOR", config.toString());

        Flower flower = new Flower();
        flower.registerFlow(MainIngestorFlow.class);
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.registerFlow(VideoChunkManagerFlow.class);
        flower.registerFlow(LoadChunkDurationFlow.class);
        flower.registerFlow(AnalyzeAndMergeChunkRangeFlow.class);
        flower.registerFlow(MergeChunkSubRangeFlow.class);

        flower.registerEventProfile(FlowTerminationEvent.class, true);
        flower.registerEventProfile(FlowExceptionEvent.class, true);
        FlowTerminationEvent.setNOTIFIER(heliumEventNotifier);
        FlowExceptionEvent.setNOTIFIER(heliumEventNotifier);

        flower.initialize();

        FlowExec<MainIngestorFlow> flowExec = flower.getFlowExec(MainIngestorFlow.class);

        MainIngestorFlow testFlow = new MainIngestorFlow(config, heliumEventNotifier);
        FlowFuture<MainIngestorFlow> flowFuture = flowExec.runFlow(testFlow);

        String eventTitleStarted = String.format("HELIUM INGESTOR started. Main flow: [%s]", flowFuture.getFlowId());
        heliumEventNotifier.notifyEvent(HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleStarted, config.toString());

        // Wait until main flow is over (not going to happen, probably)
        MainIngestorFlow flow = flowFuture.getFuture().get();

        String eventTitleShuttingDown = String.format("HELIUM INGESTOR shutting down. Main flow done: [%s]", flowFuture.getFlowId());
        heliumEventNotifier.notifyEvent(HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleShuttingDown, config.toString());

        flower.shutdownScheduler();

        String eventTitleShutdown = "HELIUM INGESTOR shut down.";
        heliumEventNotifier.notifyEvent(HeliumEventType.HELIUM_INGESTOR_PROCESS, null,
                eventTitleShutdown, config.toString());
    }
}

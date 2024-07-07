package com.helium.ingestor;

import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.core.DatedFileHeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.flows.*;
import java.io.File;
import java.util.concurrent.ExecutionException;

public class HeliumIngestorService {
    public static void run(Config config) throws ExecutionException, InterruptedException {
        if (config.cameras() == null || config.cameras().isEmpty()) {
            throw new IllegalArgumentException("Cameras not set");
        }

        HeliumEventNotifier heliumEventNotifier =
                new DatedFileHeliumEventNotifier(new File(new File(config.videoFeedFolder()), "events"));

        Flower flower = new Flower();
        flower.registerFlow(MainIngestorFlow.class);
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.registerFlow(VideoChunkManagerFlow.class);
        flower.registerFlow(LoadChunkDurationFlow.class);

        flower.registerEventProfile(FlowTerminationEvents.class, true);
        FlowTerminationEvents.setNOTIFIER(heliumEventNotifier);

        flower.initialize();

        FlowExec<MainIngestorFlow> flowExec = flower.getFlowExec(MainIngestorFlow.class);

        MainIngestorFlow testFlow = new MainIngestorFlow(config, heliumEventNotifier);
        FlowFuture<MainIngestorFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        MainIngestorFlow flow = flowFuture.getFuture().get();

        System.out.println("Flows done. " + flow);

        flower.shutdownScheduler();
    }
}

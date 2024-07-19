package com.flower.flows;

import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.flows.LoadChunkVideoDurationFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class LoadChunkVideoDurationFlowTest {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        Flower flower = new Flower();
        flower.registerFlow(LoadChunkVideoDurationFlow.class);
        flower.initialize();

        FlowExec<LoadChunkVideoDurationFlow> flowExec = flower.getFlowExec(LoadChunkVideoDurationFlow.class);

        LoadChunkVideoDurationFlow testFlow =
            new LoadChunkVideoDurationFlow("dummy_cam",
//                    "/home/john/cam/camera_20/video_2024-07-05_17_48_32.mp4",
                    "/home/john/cam/camera_20/video_2024-07-05_16_17_22.mp4",
                    new TestLogHeliumEventNotifier());

        FlowFuture<LoadChunkVideoDurationFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        LoadChunkVideoDurationFlow flow = flowFuture.getFuture().get();

        System.out.println("Flow done. " + flow);
    }
}

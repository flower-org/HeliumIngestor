package com.flower.flows;

import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.core.LogHeliumEventNotifier;
import com.helium.ingestor.flows.CameraProcessRunnerFlow;
import java.util.concurrent.ExecutionException;

public class ExternalProcessRunnerFlowTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Flower flower = new Flower();
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.initialize();

        FlowExec<CameraProcessRunnerFlow> flowExec = flower.getFlowExec(CameraProcessRunnerFlow.class);

        CameraProcessRunnerFlow testFlow = new CameraProcessRunnerFlow("Test process", "/usr/bin/ffmpeg -h", new LogHeliumEventNotifier());
        FlowFuture<CameraProcessRunnerFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        CameraProcessRunnerFlow flow = flowFuture.getFuture().get();

        System.out.println("Flow done. " + flow);
    }
}

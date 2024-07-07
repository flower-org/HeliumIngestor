package com.flower.flows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.flows.CameraProcessRunnerFlow;
import com.helium.ingestor.flows.MainIngestorFlow;
import java.util.concurrent.ExecutionException;

public class MainIngestorFlowTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException, JsonProcessingException {
        Flower flower = new Flower();
        flower.registerFlow(MainIngestorFlow.class);
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.initialize();

        FlowExec<MainIngestorFlow> flowExec = flower.getFlowExec(MainIngestorFlow.class);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new GuavaModule());

        String configStr = "videoFeedFolder: \"/home/john/cam\"\n" +
                            "cameras:\n" +
                            "  - name: \"camera_20\"\n" +
                            "    hostname: \"192.168.1.20\"\n" +
                            "    type: \"REOLINK\"\n" +
                            "    credentials:\n" +
                            "      username: \"admin\"\n" +
                            "      password: \"1111111\"\n";
        System.out.println(configStr);
        Config config = mapper.readValue(configStr, Config.class);

        if (config.cameras() == null || config.cameras().isEmpty()) {
            throw new IllegalArgumentException("Cameras not set");
        }

        MainIngestorFlow testFlow = new MainIngestorFlow(config, new TestLogHeliumEventNotifier());
        FlowFuture<MainIngestorFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        MainIngestorFlow flow = flowFuture.getFuture().get();

        System.out.println("Flow done. " + flow);
    }
}

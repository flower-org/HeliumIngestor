package com.flower.flows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.flows.VideoFootageArchiverFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class VideoFootageArchiverFlowTest {
//    @Test
    public void test() throws ExecutionException, InterruptedException, JsonProcessingException {
        Flower flower = new Flower();
        flower.registerFlow(VideoFootageArchiverFlow.class);
        flower.initialize();

        FlowExec<VideoFootageArchiverFlow> flowExec = flower.getFlowExec(VideoFootageArchiverFlow.class);

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

        VideoFootageArchiverFlow testFlow = new VideoFootageArchiverFlow(config, new TestLogHeliumEventNotifier());

        FlowFuture<VideoFootageArchiverFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        VideoFootageArchiverFlow flow = flowFuture.getFuture().get();

        System.out.println("Flow done. " + flow);
    }
}

package com.flower.flows;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        ObjectMapper mapper = new ObjectMapper()
                .configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .registerModule(new GuavaModule());
        Config config = mapper.readValue(
                "{\n" +
                        "\t\"videoFeedFolder\": \"/home/john/cam\",\n" +
                        "\t\"cameras\": [\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"camera_20\",\n" +
                        "\t\t\t\"hostname\": \"192.168.1.20\",\n" +
                        "\t\t\t\"type\": \"REOLINK\",\n" +
                        "\t\t\t\"credentials\": {\n" +
                        "\t\t\t\t\"username\": \"admin\",\n" +
                        "\t\t\t\t\"password\": \"1111111\"\n" +
                        "\t\t\t}\n" +
                        "\t\t}\n" +
                        "\t]\n" +
                        "\t\n" +
                        "}",
                Config.class);

        if (config.cameras() == null || config.cameras().isEmpty()) {
            throw new IllegalArgumentException("Cameras not set");
        }

        MainIngestorFlow testFlow = new MainIngestorFlow(config);
        FlowFuture<MainIngestorFlow> flowFuture = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture.getFlowId());

        MainIngestorFlow flow = flowFuture.getFuture().get();

        System.out.println("Flow done. " + flow);
    }
}

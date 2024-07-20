package com.flower.flows;

import com.flower.conf.FlowExec;
import com.flower.engine.Flower;
import com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow;
import com.helium.ingestor.flows.CameraProcessRunnerFlow;
import com.helium.ingestor.flows.LoadChunkVideoDurationFlow;
import com.helium.ingestor.flows.LoadMediaChannelsFlow;
import com.helium.ingestor.flows.LoadVideoDurationFlow;
import com.helium.ingestor.flows.MainIngestorFlow;
import com.helium.ingestor.flows.MergeChunkSubRangeFlow;
import com.helium.ingestor.flows.VideoChunkManagerFlow;
import org.junit.jupiter.api.Test;

public class DiagramsMermaidOutputTest {
    @Test
    public void test() {
        Flower flower = new Flower();
        flower.registerFlow(MainIngestorFlow.class);
        flower.registerFlow(CameraProcessRunnerFlow.class);
        flower.registerFlow(VideoChunkManagerFlow.class);
        flower.registerFlow(LoadChunkVideoDurationFlow.class);
        flower.registerFlow(AnalyzeAndMergeChunkRangeFlow.class);
        flower.registerFlow(MergeChunkSubRangeFlow.class);
        flower.registerFlow(LoadVideoDurationFlow.class);
        flower.registerFlow(LoadMediaChannelsFlow.class);
        flower.initialize();

        outputDiagram(flower, MainIngestorFlow.class);
        outputDiagram(flower, CameraProcessRunnerFlow.class);
        outputDiagram(flower, VideoChunkManagerFlow.class);
        outputDiagram(flower, LoadChunkVideoDurationFlow.class);
        outputDiagram(flower, AnalyzeAndMergeChunkRangeFlow.class);
        outputDiagram(flower, MergeChunkSubRangeFlow.class);
        outputDiagram(flower, LoadVideoDurationFlow.class);
        outputDiagram(flower, LoadMediaChannelsFlow.class);
    }

    static <T> void outputDiagram(Flower flower, Class<T> clz) {
        System.out.println(flower.getFlowExec(clz).buildMermaidGraph());
    }
}

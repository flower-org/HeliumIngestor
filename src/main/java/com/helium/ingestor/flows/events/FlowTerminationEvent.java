package com.helium.ingestor.flows.events;

import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.State;
import com.flower.anno.params.common.In;
import com.flower.anno.params.events.FlowInfo;
import com.flower.conf.FlowInfoPrm;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.annotation.Nullable;

import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;

/** Don't forget to call FlowTerminationEvent.setNOTIFIER(...) before use */
@EventProfileContainer
public class FlowTerminationEvent {
    @State @Nullable static HeliumEventNotifier NOTIFIER;
    public static void setNOTIFIER(HeliumEventNotifier NOTIFIER) {
        FlowTerminationEvent.NOTIFIER = NOTIFIER;
    }

    @EventFunction(types = {EventType.AFTER_FLOW})
    static void AFTER_FLOW(@In(throwIfNull = true) HeliumEventNotifier NOTIFIER,
                           @FlowInfo FlowInfoPrm flowInfo) {
        String eventTitle = String.format("!!!FLOW FINISHED!!! Flow id [%s] name [%s] type [%s]",
                flowInfo.flowId(), flowInfo.flowType(), flowInfo.flowName());
        String message = String.format("Flow terminated normally. Flow: %s.", flowInfo);
        NOTIFIER.notifyEvent(HELIUM_INGESTOR, HeliumEventType.FLOW_SHUTDOWN, null, eventTitle, message);
    }

    public static String getStackTraceAsString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        throwable.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}

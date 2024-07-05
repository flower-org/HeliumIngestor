package com.helium.ingestor.flows;

import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.State;
import com.flower.anno.params.common.In;
import com.flower.anno.params.events.FlowException;
import com.flower.anno.params.events.FlowInfo;
import com.flower.conf.FlowInfoPrm;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;

/** Don't forget to call FlowTerminationEvents.setNOTIFIER(...) before use */
@EventProfileContainer(name = "FlowTerminationEventProfile")
public class FlowTerminationEvents {
    @State @Nullable static HeliumEventNotifier NOTIFIER;

    public static void setNOTIFIER(HeliumEventNotifier NOTIFIER) {
        FlowTerminationEvents.NOTIFIER = NOTIFIER;
    }

    @EventFunction(types = {EventType.FLOW_EXCEPTION})
    static void FLOW_EXCEPTION(@In(throwIfNull = true) HeliumEventNotifier NOTIFIER,
                               @FlowException Throwable e,
                               @FlowInfo FlowInfoPrm flowInfoPrm) {
        String message = String.format("Flow crashed with exception! Flow: %s. Exception %s", flowInfoPrm, getStackTraceAsString(e));
        NOTIFIER.notifyEvent(HeliumEventType.FLOW_DIED, null, "!!!FLOW DIED!!!", message);
    }

    @EventFunction(types = {EventType.AFTER_FLOW})
    static void AFTER_FLOW(@In(throwIfNull = true) HeliumEventNotifier NOTIFIER,
                           @FlowInfo FlowInfoPrm flowInfoPrm) {
        String message = String.format("Flow terminated normally. Flow: %s.", flowInfoPrm);
        NOTIFIER.notifyEvent(HeliumEventType.FLOW_DIED, null, "!!!FLOW FINISHED!!!", message);
    }

    public static String getStackTraceAsString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        throwable.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}

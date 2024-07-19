package com.helium.ingestor.flows.events;

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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;

/** Don't forget to call FlowExceptionEvent.setNOTIFIER(...) before use */
@EventProfileContainer
public class FlowExceptionEvent {
    @State @Nullable static HeliumEventNotifier NOTIFIER;
    public static void setNOTIFIER(HeliumEventNotifier NOTIFIER) {
        FlowExceptionEvent.NOTIFIER = NOTIFIER;
    }

    @EventFunction(types = {EventType.FLOW_EXCEPTION})
    static void FLOW_EXCEPTION(@In(throwIfNull = true) HeliumEventNotifier NOTIFIER,
                               @FlowException Throwable e,
                               @FlowInfo FlowInfoPrm flowInfo) {
        String eventTitle = String.format("!!!FLOW EXCEPTION!!! Flow id [%s] name [%s] type [%s]",
                flowInfo.flowId(), flowInfo.flowType(), flowInfo.flowName());
        String message = String.format("Flow crashed with exception! Flow: %s. Exception %s", flowInfo, getStackTraceAsString(e));
        NOTIFIER.notifyEvent(HELIUM_INGESTOR, HeliumEventType.FLOW_EXCEPTION, null, eventTitle, message);
    }
}

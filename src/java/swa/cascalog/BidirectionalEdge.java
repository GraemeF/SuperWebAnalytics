package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class BidirectionalEdge extends CascalogFunction {
       public void operate(FlowProcess process, FunctionCall call) {
            Object node1 = call.getArguments().getObject(0);
            Object node2 = call.getArguments().getObject(1);
            if(!node1.equals(node2)) {
                call.getOutputCollector().add(
                        new Tuple(node1, node2));
                call.getOutputCollector().add(
                        new Tuple(node2, node1));
            }
       }
}

package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import swa.generated.Data;
import swa.generated.EquivEdge;

/**
 * Created with IntelliJ IDEA.
 * User: davek
 * Date: 11/16/12
 * Time: 11:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class EdgifyEquiv extends CascalogFunction {
    public void operate(FlowProcess process, FunctionCall call) {
        Data data = (Data) call.getArguments().getObject(0);
        EquivEdge equiv = data.get_dataunit().get_equiv();
        call.getOutputCollector().add(
                new Tuple(equiv.get_id1(), equiv.get_id2()));
    }
}

package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import swa.generated.Data;
import swa.generated.PersonID;

public class MakeNormalizedPageview extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            PersonID newId = (PersonID) call.getArguments()
                    .getObject(0);
            Data data = ((Data) call.getArguments().getObject(1))
                    .deepCopy();
            if(newId!=null) {
                data.get_dataunit().get_page_view().set_person(newId);
            }
            call.getOutputCollector().add(new Tuple(data));
        }
    }

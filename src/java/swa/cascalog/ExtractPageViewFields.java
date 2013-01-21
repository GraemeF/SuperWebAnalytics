package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import swa.generated.Data;
import swa.generated.PageID;
import swa.generated.PageViewEdge;

public class ExtractPageViewFields extends CascalogFunction {

        public void operate(FlowProcess process, FunctionCall call) {
            Data data = (Data) call.getArguments().getObject(0);
            PageViewEdge pageview = data.get_dataunit()
                    .get_page_view();
            if(pageview.get_page().getSetField() ==
                    PageID._Fields.URL) {
                call.getOutputCollector().add(new Tuple(
                        pageview.get_page().get_url(),
                        pageview.get_person(),
                        data.get_pedigree().get_true_as_of_secs()
                ));
            }
        }
}

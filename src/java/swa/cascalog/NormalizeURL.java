package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import swa.generated.Data;
import swa.generated.DataUnit;
import swa.generated.PageID;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: davek
 * Date: 11/16/12
 * Time: 11:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class NormalizeURL extends CascalogFunction {
    public void operate(FlowProcess process, FunctionCall call) {
        Data data = ((Data) call.getArguments()
                .getObject(0)).deepCopy();
        DataUnit du = data.get_dataunit();
        if(du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
            normalize(du.get_page_view().get_page());
        } else if(du.getSetField() ==
                DataUnit._Fields.PAGE_PROPERTY) {
            normalize(du.get_page_property().get_id());
        }
        call.getOutputCollector().add(new Tuple(data));
    }

    private void normalize(PageID page) {
        if(page.getSetField() == PageID._Fields.URL) {
            String urlStr = page.get_url();
            try {
                URL url = new URL(urlStr);
                page.set_url(url.getProtocol() + "://" +
                        url.getHost() + url.getPath());
            } catch(MalformedURLException e) {
            }
        }
    }
}


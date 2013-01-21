package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.net.MalformedURLException;
import java.net.URL;

public class ExtractDomain extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            String urlStr = call.getArguments().getString(0);
            try {
                URL url = new URL(urlStr);
                call.getOutputCollector().add(
                        new Tuple(url.getAuthority()));
            } catch(MalformedURLException e) {
            }
        }
}


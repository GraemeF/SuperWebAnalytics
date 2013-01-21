package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;
import java.util.Iterator;

public class ConstructHyperLogLog extends CascalogBuffer {
        public void operate(FlowProcess process, BufferCall call) {
            HyperLogLog hll = new HyperLogLog(8000);
            Iterator<TupleEntry> it = call.getArgumentsIterator();
            while(it.hasNext()) {
                TupleEntry tuple = it.next();
                hll.offer(tuple.getObject(0));
            }
            try {
                call.getOutputCollector().add(
                        new Tuple(hll.getBytes()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

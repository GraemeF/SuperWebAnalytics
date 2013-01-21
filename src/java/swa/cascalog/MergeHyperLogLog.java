package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;
import java.util.Iterator;

public class MergeHyperLogLog extends CascalogBuffer {
        public void operate(FlowProcess process, BufferCall call) {
            Iterator<TupleEntry> it = call.getArgumentsIterator();
            HyperLogLog curr = null;
            try {
                while(it.hasNext()) {
                    TupleEntry tuple = it.next();
                    byte[] serialized = (byte[]) tuple.getObject(0);
                    HyperLogLog hll = HyperLogLog.Builder.build(
                            serialized);
                    if(curr==null) {
                        curr = hll;
                    } else {
                        curr = (HyperLogLog) curr.merge(hll);
                    }
                }
                call.getOutputCollector().add(
                        new Tuple(curr.getBytes()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch(CardinalityMergeException e) {
                throw new RuntimeException(e);
            }
        }
    }

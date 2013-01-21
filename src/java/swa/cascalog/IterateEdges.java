package swa.cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import swa.generated.PersonID;

import java.util.Iterator;
import java.util.TreeSet;

public class IterateEdges extends CascalogBuffer {
        public void operate(FlowProcess process, BufferCall call) {
            PersonID grouped = (PersonID) call.getGroup()
                    .getObject(0);
            TreeSet<PersonID> allIds = new TreeSet<PersonID>();
            allIds.add(grouped);
            Iterator<TupleEntry> it = call.getArgumentsIterator();
            while(it.hasNext()) {
                allIds.add((PersonID) it.next().getObject(0));
            }
            Iterator<PersonID> allIdsIt = allIds.iterator();
            PersonID smallest = allIdsIt.next();
            boolean isProgress = allIds.size() > 2 &&
                    !grouped.equals(smallest);
            while(allIdsIt.hasNext()) {
                PersonID id = allIdsIt.next();
                call.getOutputCollector().add(
                        new Tuple(smallest, id, isProgress));
            }
        }
}

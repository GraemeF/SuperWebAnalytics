package swa.pail;

import swa.generated.Data;

public class DataPailStructure extends ThriftPailStructure<Data> {
    @Override
    protected Data createThriftObject() {
        return new Data();
    }

    @Override
    public Class getType() {
        return Data.class;
    }
}

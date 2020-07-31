package org.embulk.base.restclient.record;

public abstract class ServiceRecord {
    public abstract ServiceValue getValue(ValueLocator locator);

    @Override
    public abstract String toString();

    public abstract static class Builder {
        public abstract void reset();

        public abstract void add(ServiceValue serviceValue, ValueLocator valueLocator);

        public abstract ServiceRecord build();
    }
}

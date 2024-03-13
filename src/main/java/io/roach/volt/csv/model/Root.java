package io.roach.volt.csv.model;

public class Root {
    private ApplicationModel model;

    public Root(ApplicationModel model) {
        this.model = model;
    }

    public ApplicationModel getModel() {
        return model;
    }

    public Root setModel(ApplicationModel model) {
        this.model = model;
        return this;
    }
}

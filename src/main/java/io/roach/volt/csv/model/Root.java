package io.roach.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder(value = {"springModel","applicationModel"})
public class Root {
    @JsonProperty("spring")
    private SpringModel springModel;

    @JsonProperty("model")
    private ApplicationModel applicationModel;

    public Root(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    public ApplicationModel getApplicationModel() {
        return applicationModel;
    }

    public Root setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        return this;
    }

    public SpringModel getSpringModel() {
        return springModel;
    }

    public Root setSpringModel(SpringModel springModel) {
        this.springModel = springModel;
        return this;
    }
}

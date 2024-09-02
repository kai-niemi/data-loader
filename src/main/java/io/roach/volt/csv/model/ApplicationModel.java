package io.roach.volt.csv.model;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

@Validated
@ConfigurationProperties(prefix = "model")
public class ApplicationModel {
    @NotNull
    private String outputPath;

    private List<Table> tables = new ArrayList<>();

    @JsonProperty("import-into")
    private ImportInto importInto;

    public ImportInto getImportInto() {
        if (importInto == null) {
            importInto =  ImportInto.createDefault();
        }
        return importInto;
    }

    public void setImportInto(ImportInto importInto) {
        this.importInto = importInto;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
}

package io.cockroachdb.dlr.core.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotNull;

@Validated
@ConfigurationProperties(prefix = "model")
public class ApplicationModel {
    @NotNull
    private String outputPath;

    private List<Table> tables = new ArrayList<>();

    private ImportInto importInto;

    private Map<ImportOption, String> options = new LinkedHashMap<>();

    public Map<ImportOption, String> getOptions() {
        return options;
    }

    public void setOptions(Map<ImportOption, String> options) {
        this.options = options;
    }

    public ImportInto getImportInto() {
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

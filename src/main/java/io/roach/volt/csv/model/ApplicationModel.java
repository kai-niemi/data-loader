package io.roach.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "model")
public class ApplicationModel {
    @NotNull
    private String outputPath;

    private Boolean append;

    @Pattern(regexp = "^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)\\s?([kKmMgG]+)")
    private String count;

    @JsonProperty("import")
    private ImportSettings importSettings = ImportSettings.createDefault();

    private List<Table> tables = new ArrayList<>();

    public ImportSettings getImport() {
        return importSettings;
    }

    public void setImport(ImportSettings importSettings) {
        this.importSettings = importSettings;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public Boolean isAppend() {
        return append;
    }

    public void setAppend(Boolean append) {
        this.append = append;
    }
}

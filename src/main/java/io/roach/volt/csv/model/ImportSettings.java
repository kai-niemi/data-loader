package io.roach.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.util.LinkedHashMap;
import java.util.Map;

public class ImportSettings {
    public static ImportSettings createDefault() {
        ImportSettings importSettings = new ImportSettings();
        importSettings.setFile("import.sql");
        importSettings.setPrefix("http://${local-ip}:8090/");

        importSettings.options.put(ImportOption.delimiter, ",");
        importSettings.options.put(ImportOption.fields_enclosed_by, "");
        importSettings.options.put(ImportOption.skip, "1");
        importSettings.options.put(ImportOption.nullif, "");
        importSettings.options.put(ImportOption.allow_quoted_null, "null");

        return importSettings;
    }

    @NotBlank
    @JsonProperty("file")
    private String file;

    @NotBlank
    @JsonProperty("prefix")
    private String prefix;

    private Map<ImportOption, String> options = new LinkedHashMap<>();

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Map<ImportOption, String> getOptions() {
        return options;
    }

    public void setOptions(Map<ImportOption, String> options) {
        this.options = options;
    }
}

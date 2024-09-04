package io.roach.volt.csv.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;

public class ImportInto {
    public static ImportInto createDefault() {
        ImportInto importInto = new ImportInto();
        importInto.options.putAll(createDefaultOptions());
        importInto.setFile("import.sql");
        importInto.setPrefix("http://${local-ip}:8090/");
        return importInto;
    }

    public static Map<ImportOption, String> createDefaultOptions() {
        Map<ImportOption, String> options = new LinkedHashMap<>();
        options.put(ImportOption.delimiter, ",");
        options.put(ImportOption.fields_enclosed_by, "(empty)");
        options.put(ImportOption.skip, "1");
        options.put(ImportOption.allow_quoted_null, "null");
        return options;
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

    @JsonIgnore
    public Path getPath() {
        return Paths.get(file);
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

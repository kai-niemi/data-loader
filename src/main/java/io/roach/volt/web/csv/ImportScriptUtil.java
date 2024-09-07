package io.roach.volt.web.csv;

import java.util.stream.Collectors;

import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.roach.volt.csv.model.Column;
import io.roach.volt.web.TableModel;

public abstract class ImportScriptUtil {
    private ImportScriptUtil() {
    }

    public static String generateImportInto(TableModel tableModel) {
        String uri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .pathSegment("table", "schema", tableModel.getTable())
                .buildAndExpand()
                .toUriString();

        return "IMPORT INTO "
                + tableModel.getTable()
                + "("
                + tableModel.getColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.joining(","))
                + ") CSV DATA ( '" + uri + "');";
    }
}

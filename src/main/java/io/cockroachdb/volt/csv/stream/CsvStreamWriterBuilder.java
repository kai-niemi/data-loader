package io.cockroachdb.volt.csv.stream;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.util.StringUtils;

public class CsvStreamWriterBuilder<W> {
    private String delimiter = ",";

    private String quoteCharacter = "";

    private List<String> columnNames = List.of();

    private boolean includeHeader;

    public CsvStreamWriterBuilder<W> withIncludeHeader(boolean includeHeader) {
        this.includeHeader = includeHeader;
        return this;
    }

    public CsvStreamWriterBuilder<W> withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public CsvStreamWriterBuilder<W> withQuoteCharacter(String quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
        return this;
    }

    public CsvStreamWriterBuilder<W> withColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    public CsvStreamWriter<W> build() throws IOException {
        DelimitedLineAggregator<W> lineAggregator
                = new DelimitedLineAggregator<>();
        lineAggregator.setQuoteCharacter(quoteCharacter);
        lineAggregator.setDelimiter(delimiter);

        CsvStreamWriter<W> itemWriter = new CsvStreamWriter<>();
        itemWriter.setLineAggregator(lineAggregator);
        if (includeHeader) {
            itemWriter.setHeaderCallback(writer -> writer
                    .write(StringUtils.collectionToDelimitedString(columnNames, delimiter))
            );
        }

        return itemWriter;
    }
}

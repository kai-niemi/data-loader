package io.roach.volt.csv.producer;

import io.roach.volt.csv.model.Column;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.StringUtils;

import java.nio.file.Path;
import java.util.List;

public class ChunkFileWriter<W> implements ChunkWriter<W> {
    private final String delimiter;

    private final String quoteCharacter;

    private final boolean append;

    private final Path outputPath;

    private FlatFileItemWriter<W> itemWriter;

    public ChunkFileWriter(String delimiter,
                           String quoteCharacter,
                           boolean append,
                           Path outputPath) {
        this.delimiter = delimiter;
        this.quoteCharacter = quoteCharacter;
        this.append = append;
        this.outputPath = outputPath;
    }

    @Override
    public void open(List<Column> columns) {
        DelimitedLineAggregator<W> lineAggregator
                = new DelimitedLineAggregator<>();
        lineAggregator.setQuoteCharacter(quoteCharacter);
        lineAggregator.setDelimiter(delimiter);

        // Get names of visible columns only
        List<String> visibleCols = columns.stream()
                .filter(column -> column.isHidden() == null || !column.isHidden())
                .map(Column::getName)
                .toList();

        this.itemWriter = new FlatFileItemWriterBuilder<W>()
                .name("volt-csv")
                .append(append)
                .shouldDeleteIfExists(!append)
                .saveState(false)
                .transactional(false)
                .forceSync(false)
                .resource(new FileSystemResource(outputPath))
                .lineAggregator(lineAggregator)
                .headerCallback(writer ->
                        writer.write(StringUtils.collectionToDelimitedString(visibleCols, delimiter)))
                .build();

        this.itemWriter.open(new ExecutionContext());
    }

    @Override
    public void writeChunk(Chunk<? extends W> values) {
        try {
            this.itemWriter.write(values);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ItemStreamException("Uncategorized exception", e);
        }
    }

    @Override
    public void close() {
        if (itemWriter != null) {
            this.itemWriter.close();
        }
    }
}

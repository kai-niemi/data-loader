package io.cockroachdb.volt.csv.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class CsvStreamWriter<T> extends AbstractItemStreamItemWriter<T>
        implements InitializingBean, Closeable {
    public static final String DEFAULT_LINE_SEPARATOR = System.lineSeparator();

    private String lineSeparator = DEFAULT_LINE_SEPARATOR;

    private LineAggregator<T> lineAggregator;

    private FlatFileHeaderCallback headerCallback;

    private FlatFileFooterCallback footerCallback;

    private Writer writer;

    public CsvStreamWriter() {
        this.setExecutionContextName(ClassUtils.getShortName(CsvStreamWriter.class));
    }

    public void setLineAggregator(LineAggregator<T> lineAggregator) {
        this.lineAggregator = lineAggregator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public void setHeaderCallback(FlatFileHeaderCallback headerCallback) {
        this.headerCallback = headerCallback;
    }

    public void setFooterCallback(FlatFileFooterCallback footerCallback) {
        this.footerCallback = footerCallback;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(lineAggregator, "A LineAggregator must be provided.");
    }

    /**
     * Writes out a string followed by a "new line", where the format of the new
     * line separator is determined by the underlying operating system.
     *
     * @param chunk list of items to be written to output stream
     * @throws WriteFailedException if an error occurs while writing items to the output stream
     */
    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        String lines = doWrite(chunk);
        try {
            writer.write(lines);
            writer.flush();
        } catch (IOException e) {
            throw new WriteFailedException("Could not write data. The stream may be corrupt.", e);
        }
    }

    /**
     * Write out a string of items followed by a "new line", where the format of the new
     * line separator is determined by the underlying operating system.
     *
     * @param chunk to be written
     * @return written lines
     */
    protected String doWrite(Chunk<? extends T> chunk) {
        StringBuilder lines = new StringBuilder();
        for (T item : chunk) {
            lines.append(this.lineAggregator.aggregate(item))
                    .append(this.lineSeparator);
        }
        return lines.toString();
    }

    /**
     * @see ItemStream#open(ExecutionContext)
     */
    @SuppressWarnings("removal")
    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);

        if (headerCallback != null) {
            try {
                headerCallback.writeHeader(writer);
                writer.write(lineSeparator);
            } catch (IOException e) {
                throw new ItemStreamException("Could not write headers.  The file may be corrupt.", e);
            }
        }
    }

    /**
     * @see ItemStream#close()
     */
    @SuppressWarnings("removal")
    @Override
    public void close() {
        super.close();
        try {
            if (footerCallback != null) {
                footerCallback.writeFooter(writer);
                writer.flush();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to write footer before closing", e);
        }
    }
}

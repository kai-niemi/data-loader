package io.cockroachdb.volt.shell.support;

import io.cockroachdb.volt.csv.model.ApplicationModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class DirectoryValueProvider implements ValueProvider {
    @Autowired
    private ApplicationModel applicationModel;

    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        String input = completionContext.currentWordUpToCursor();
        int lastSlash = input.lastIndexOf(File.separatorChar);
        Path dir = lastSlash > -1 ? Paths.get(input.substring(0, lastSlash + 1)) : Paths.get(applicationModel.getOutputPath());
        String prefix = input.substring(lastSlash + 1);

        try {
            return Files.find(dir, 99, (p, a) -> p.getFileName() != null
                                    && p.getFileName().toString().startsWith(prefix)
                                    && Files.isDirectory(p),
                            FOLLOW_LINKS)
                    .map(p -> {
                        String value = p.toString().replace(File.separator, "/");
                        return new CompletionProposal(value).complete(true);
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

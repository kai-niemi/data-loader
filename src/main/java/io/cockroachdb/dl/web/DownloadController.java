package io.cockroachdb.dl.web;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.Link;
import org.springframework.http.CacheControl;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.cockroachdb.dl.config.ProfileNames;
import io.cockroachdb.dl.core.model.ApplicationModel;
import io.cockroachdb.dl.core.model.ImportInto;
import io.cockroachdb.dl.util.Gzip;
import io.cockroachdb.dl.web.model.MessageModel;

@RestController
@Profile(ProfileNames.STREAMING)
public class DownloadController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationModel applicationModel;

    @GetMapping("/download")
    public ResponseEntity<MessageModel> index() throws IOException {
        MessageModel model = MessageModel.from("Local files");

        final Path basePath = Paths.get(applicationModel.getOutputPath());
        if (!Files.isDirectory(basePath)) {
            return ResponseEntity.ok(model);
        }

        ImportInto importInto = applicationModel.getImportInto();
        if (importInto != null) {
            if (Files.isRegularFile(importInto.getPath())) {
                model.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                                .pathSegment(importInto.getFile())
                                .buildAndExpand()
                                .toUriString())
                        .withRel(LinkRelations.DOWNLOAD_REL)
                        .withTitle(importInto.getFile())
                        .withType(MediaType.TEXT_PLAIN_VALUE)
                );
            }
        }

        for (Path path : findFiles(basePath)) {
            // Avoid URI encoding of separators
            model.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                            .pathSegment(path.toString())
                            .buildAndExpand()
                            .toUriString())
                    .withRel(LinkRelations.DOWNLOAD_REL)
                    .withTitle(path.getFileName().toString())
                    .withType(MediaType.TEXT_PLAIN_VALUE)
            );
        }

        return ResponseEntity.ok(model);
    }

    private List<Path> findFiles(Path basePath) throws IOException {
        List<Path> importFiles = new ArrayList<>();

        final PathMatcher matcher = FileSystems.getDefault()
                .getPathMatcher("glob:*{.csv,.sql,*.yml}");

        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                Path fileName = path.getFileName();
                if (fileName != null && matcher.matches(fileName)) {
                    path = basePath.toAbsolutePath().relativize(path.toAbsolutePath());
                    importFiles.add(path);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return importFiles;
    }

    @RequestMapping(value = "/download/{*filename}")
    public ResponseEntity<StreamingResponseBody> downloadFile(
            @PathVariable("filename") String path,
            @RequestHeader(value = HttpHeaders.ACCEPT_ENCODING, required = false, defaultValue = "")
            String acceptEncoding) throws FileNotFoundException {

        final Path absolutePath = Paths.get(applicationModel.getOutputPath())
                .resolve(path.substring(1));

        logger.debug("Download request for: %s (%s)".formatted(path, absolutePath.toString()));

        if (!Files.isRegularFile(absolutePath)) {
            throw new FileNotFoundException(absolutePath.toString());
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setCacheControl(CacheControl
                .noCache()
                .noTransform()
                .mustRevalidate());
        headers.setContentDisposition(ContentDisposition
                .inline()
                .filename(path)
                .build());

        boolean gzip = acceptEncoding.contains("gzip");
        if (gzip) {
            // For some reason CockroachDB complains on "bad header" if gzip encoding is ack:ed
            headers.set(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        return ResponseEntity.ok()
                .headers(headers)
                .body(out -> {
                    if (gzip) {
                        Gzip.copy(new FileInputStream(absolutePath.toFile()), out);
                    } else {
                        Files.copy(absolutePath, out);
                    }
                });
    }
}

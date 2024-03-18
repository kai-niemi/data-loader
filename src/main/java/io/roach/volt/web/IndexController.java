package io.roach.volt.web;

import io.roach.volt.config.ProfileNames;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.util.RandomData;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.Link;
import org.springframework.http.CacheControl;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

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

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@RequestMapping(path = "/")
@Profile(ProfileNames.PROXY)
public class IndexController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    private BuildProperties buildProperties;

    @GetMapping
    public ResponseEntity<MessageModel> index() throws IOException {
        String title = buildProperties.getName();
        String version = buildProperties.getVersion();

        MessageModel index = MessageModel.from("Welcome to %s %s".formatted(title, version));
        index.setNotice(RandomData.randomRoachFact());

        index.add(linkTo(methodOn(getClass())
                .importFiles())
                .withRel(LinkRelations.FILES_REL)
                .withTitle("Import file index"));

        index.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                        .pathSegment("actuator")
                        .buildAndExpand()
                        .toUriString())
                .withRel(LinkRelations.ACTUATORS_REL)
                .withTitle("Spring boot actuators"));

        return ResponseEntity.ok(index);
    }

    @GetMapping("/import-files")
    public ResponseEntity<MessageModel> importFiles() throws IOException {
        MessageModel index = MessageModel.from("Available import SQL files");
        index.setNotice(RandomData.randomRoachFact());

        final Path basePath = Paths.get(applicationModel.getOutputPath());
        if (!Files.isDirectory(basePath)) {
            return ResponseEntity.ok(index);
        }

        List<Path> importFiles = new ArrayList<>();

        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            final PathMatcher matcher = FileSystems.getDefault()
                    .getPathMatcher("glob:*{.csv,.sql,*.yml}");

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

        for (Path path : importFiles) {
            // Avoid URI encoding of separators
            index.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                            .pathSegment(path.toString())
                            .buildAndExpand()
                            .toUriString())
                    .withRel(LinkRelations.IMPORT_FILE_REL)
                    .withTitle(path.getFileName().toString())
                    .withType(MediaType.TEXT_PLAIN_VALUE)
            );
        }

        return ResponseEntity.ok(index);
    }

    @RequestMapping(value = "/{*filename}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> downloadResource(
            @PathVariable("filename") String path,
            HttpServletResponse response) throws FileNotFoundException {

        final Path absolutePath = Paths.get(applicationModel.getOutputPath())
                .resolve(path.substring(1));

        logger.trace("Download request for: %s (%s)"
                .formatted(path, absolutePath.toString()));

        if (Files.isRegularFile(absolutePath)) {
            HttpHeaders headers = new HttpHeaders();
            headers.setCacheControl(CacheControl
                    .noCache()
                    .noTransform()
                    .mustRevalidate());
            headers.setContentDisposition(ContentDisposition
                    .inline()
                    .filename(path)
                    .build());

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(out -> {
                        long bytes = Files.copy(absolutePath, out);
                        response.setContentLengthLong(bytes);
                    });
        }

        throw new FileNotFoundException(absolutePath.toString());
    }
}

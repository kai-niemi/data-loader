package io.cockroachdb.dlr.web;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.Link;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.cockroachdb.dlr.config.ProfileNames;
import io.cockroachdb.dlr.util.RandomData;
import io.cockroachdb.dlr.web.model.MessageModel;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@Profile(ProfileNames.HTTP)
public class IndexController {
    @Autowired
    private BuildProperties buildProperties;

    @GetMapping("/")
    public ResponseEntity<MessageModel> index() throws IOException {
        String title = buildProperties.getName();
        String version = buildProperties.getVersion();

        MessageModel index = MessageModel.from("Welcome to %s %s".formatted(title, version));
        index.setNotice(RandomData.randomRoachFact());

        index.add(linkTo(methodOn(DownloadController.class)
                .index())
                .withRel(LinkRelations.DOWNLOAD_INDEX_REL)
                .withTitle("File download index"));

        index.add(linkTo(methodOn(TableController.class)
                .index())
                .withRel(LinkRelations.TABLE_INDEX_REL)
                .withTitle("Table export index"));

        index.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                        .pathSegment("actuator")
                        .buildAndExpand()
                        .toUriString())
                .withRel(LinkRelations.ACTUATORS_REL)
                .withTitle("Spring boot actuators"));

        return ResponseEntity.ok(index);
    }

    @GetMapping("/health")
    public ResponseEntity<MessageModel> health() {
        return ResponseEntity.ok().build();
    }
}

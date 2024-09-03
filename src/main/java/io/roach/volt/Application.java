package io.roach.volt;

import io.roach.volt.config.ProfileNames;
import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.shell.jline.InteractiveShellRunner;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Set;

@Configuration
@ConfigurationPropertiesScan(basePackageClasses = Application.class)
@SpringBootApplication(exclude = {
        BatchAutoConfiguration.class,
        JdbcRepositoriesAutoConfiguration.class,
        DataSourceAutoConfiguration.class
})
@Order(InteractiveShellRunner.PRECEDENCE - 100)
public class Application implements PromptProvider {
    private static void printHelpAndExit(String message) {
        System.out.println("Usage: java --jar volt.jar <options> [args..]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("--help                    this help");
        System.out.println("--http                    enable http listener for IMPORT/COPY commands");
        System.out.println("--profiles [profile,..]   spring profiles to activate");
        System.out.println();
        System.out.println("All other options are passed to the shell.");
        System.out.println();
        System.out.println(message);

        System.exit(0);
    }

    public static void main(String[] args) {
        LinkedList<String> argsList = new LinkedList<>(Arrays.asList(args));
        LinkedList<String> passThroughArgs = new LinkedList<>();

        Set<String> profiles =
                StringUtils.commaDelimitedListToSet(System.getProperty("spring.profiles.active", "default"));

        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.equals("--help")) {
                printHelpAndExit("");
            } else if (arg.equals("--http")) {
                profiles.add(ProfileNames.HTTP);
            } else if (arg.equals("--profiles")) {
                if (argsList.isEmpty()) {
                    printHelpAndExit("Expected list of profile names");
                }
                profiles.clear();
                profiles.addAll(StringUtils.commaDelimitedListToSet(argsList.pop()));
            } else {
                passThroughArgs.add(arg);
            }
        }

        if (!profiles.isEmpty()) {
            System.setProperty("spring.profiles.active", String.join(",", profiles));
        }

        new SpringApplicationBuilder(Application.class)
                .web(profiles.contains(ProfileNames.HTTP)
                        ? WebApplicationType.SERVLET
                        : WebApplicationType.NONE)
                .bannerMode(Banner.Mode.CONSOLE)
                .logStartupInfo(true)
                .profiles(profiles.toArray(new String[0]))
                .run(passThroughArgs.toArray(new String[] {}));
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public AttributedString getPrompt() {
        return new AttributedString("volt:$ ",
                AttributedStyle.DEFAULT
                        .foreground(AttributedStyle.GREEN | AttributedStyle.BRIGHT)
                        .blinkDefault());
    }

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @EventListener
    public void onExitEvent(GenericEvent<ExitEvent> event) {
        logger.trace("Received exit event code: " + event.getTarget().getExitCode());
        SpringApplication.exit(applicationContext, () -> 0);
    }
}

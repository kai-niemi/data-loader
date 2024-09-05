package io.roach.volt.shell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.context.InteractionMode;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.shell.standard.commands.Quit;

import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.util.AsciiArt;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class Exit implements Quit.Command {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("asyncTaskExecutor")
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Autowired
    private ApplicationContext applicationContext;

    @EventListener
    public void onExitEvent(GenericEvent<ExitEvent> event) {
        int code = event.getTarget().getExitCode();
//        exit(event.getTarget().getExitCode());
        threadPoolExecutor.shutdown();
        logger.info("Received exit event code: " + code);
        // Unclear why both of these are needed..
        SpringApplication.exit(applicationContext, () -> code);
        System.exit(code);
    }

    @ShellMethod(value = "Exit the shell", key = {"q", "x", "quit", "exit"},
            interactionMode = InteractionMode.INTERACTIVE)
    public void exit(@ShellOption(help = "exit code", defaultValue = "0") int code) {
        threadPoolExecutor.shutdown();
        logger.info("Exiting - bye! %s".formatted(AsciiArt.bye()));
        SpringApplication.exit(applicationContext, () -> code);
        System.exit(code);
//        throw new ExitRequest(code);
    }
}

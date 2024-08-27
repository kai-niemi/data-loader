package io.roach.volt.shell;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.commands.Quit;

import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.util.AsciiArt;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class Exit implements Quit.Command {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    @Qualifier("threadPoolTaskExecutor")
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Exit the shell", key = {"q", "x", "quit", "exit"})
    public void exit() {
        while (threadPoolExecutor.getActiveCount() > 0) {
            ansiConsole.magenta("Waiting for %d workers to finish"
                    .formatted(threadPoolExecutor.getActiveCount())).nl();
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        ansiConsole.magenta("Exiting - bye! %s"
                .formatted(AsciiArt.bye())).nl();

        threadPoolExecutor.initiateShutdown();

        SpringApplication.exit(applicationContext, () -> 0);

        System.exit(0);
    }
}

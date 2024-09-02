package io.roach.volt.shell;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Async;
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
    @Qualifier("asyncTaskExecutor")
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Exit the shell", key = {"q", "x", "quit", "exit"})
    public void exit() {
        threadPoolExecutor.shutdown();

        if (threadPoolExecutor.getActiveCount() > 0) {
            ansiConsole.magenta("There are still %d pending worker(s) - unable"
                    .formatted(threadPoolExecutor.getActiveCount()))
                    .nl();
            return;
        }

        ansiConsole.magenta("Exiting - bye! %s".formatted(AsciiArt.bye())).nl();

        SpringApplication.exit(applicationContext, () -> 0);
        System.exit(0);
    }
}

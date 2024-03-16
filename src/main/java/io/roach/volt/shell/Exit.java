package io.roach.volt.shell;

import io.roach.volt.shell.support.AnsiConsole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.shell.ExitRequest;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.commands.Quit;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class Exit implements Quit.Command {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Quit the non-interactive shell", key = {"q", "quit"})
    public void quit() {
        ansiConsole.magenta("Bye!").nl();
        applicationContext.close();
    }

    @ShellMethod(value = "Exit the interactive shell", key = {"x","exit"})
    public void exit() {
        ansiConsole.magenta("Bye!").nl();
        applicationContext.close();
        throw new ExitRequest();
    }
}

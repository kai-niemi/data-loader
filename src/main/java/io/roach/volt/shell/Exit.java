package io.roach.volt.shell;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.commands.Quit;

import io.roach.volt.shell.support.AnsiConsole;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class Exit implements Quit.Command {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Exit the shell", key = {"q", "x", "quit", "exit"})
    public void exit() {
        ansiConsole.magenta("Bye!").nl();
        applicationContext.close();
        System.exit(0);
    }
}

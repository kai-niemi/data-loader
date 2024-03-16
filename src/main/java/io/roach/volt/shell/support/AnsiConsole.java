package io.roach.volt.shell.support;

import org.jline.terminal.Terminal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ansi.AnsiColor;
import org.springframework.boot.ansi.AnsiOutput;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Locale;

@Component
public class AnsiConsole {
    private final Terminal terminal;

    public AnsiConsole(@Autowired @Lazy Terminal terminal) {
        Assert.notNull(terminal, "terminal is null");
        this.terminal = terminal;
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public AnsiConsole cyan(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_CYAN, format, args);
    }

    public AnsiConsole red(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_RED, format, args);
    }

    public AnsiConsole green(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_GREEN, format, args);
    }

    public AnsiConsole blue(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_BLUE, format, args);
    }

    public AnsiConsole yellow(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_YELLOW, format, args);
    }

    public AnsiConsole magenta(String format, Object... args) {
        return printf(AnsiColor.BRIGHT_MAGENTA, format, args);
    }

    private AnsiConsole printf(AnsiColor color, String format, Object... args) {
        return print(color, String.format(Locale.US, format, args));
    }

    public AnsiConsole print(AnsiColor color, String text) {
        terminal.writer().print(AnsiOutput.toString(color, text, AnsiColor.DEFAULT));
        terminal.writer().flush();
        return this;
    }

    public AnsiConsole println(AnsiColor color, String text) {
        terminal.writer().println(AnsiOutput.toString(color, text, AnsiColor.DEFAULT));
        terminal.writer().flush();
        return this;
    }

    public AnsiConsole nl() {
        terminal.writer().println();
        terminal.writer().flush();
        return this;
    }
}

package io.roach.volt.util;

import org.jline.terminal.Terminal;

public abstract class AsciiArt {
    private static final String CUU = "\u001B[A";

    private static final String DL = "\u001B[1M";

    private static final int WIDTH = 30;

    private AsciiArt() {
    }

    public static String happy() {
        return "(ʘ‿ʘ)";
    }

    public static String shrug() {
        return "¯\\_(ツ)_/¯";
    }

    public static String flipTableGently() {
        return "(╯°□°)╯︵ ┻━┻";
    }

    public static String flipTableRoughly() {
        return "(ノಠ益ಠ)ノ彡┻━┻";
    }

    public static void progressBar(Terminal terminal,
                                   long current, long total, String label) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (WIDTH * p) - 1);
        String bar = String.format(
                "%,9d/%-,9d %5.1f%% [%-30s] %s",
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">",
                label);
        terminal.writer().println(CUU + "\r" + DL + bar);
        terminal.writer().flush();
    }

    public static void progressBar(Terminal terminal,
                                   long current, long total, String label,
                                   double requestsPerSec, long remainingMillis) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (WIDTH * p) - 1);
        String bar = String.format(
                "%,9d/%-,9d %5.1f%% [%-30s] %,7.0f/s eta %s (%s)",
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">",
                requestsPerSec,
                TimeFormat.millisecondsToDisplayString(remainingMillis),
                label);
        terminal.writer().println(CUU + "\r" + DL + bar);
        terminal.writer().flush();

    }
}


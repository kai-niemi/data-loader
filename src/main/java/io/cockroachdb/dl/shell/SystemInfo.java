package io.cockroachdb.dl.shell;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import io.cockroachdb.dl.shell.support.AnsiConsole;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class SystemInfo {
    @Autowired
    @Qualifier("asyncTaskExecutor")
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Print local system information", key = {"system-info", "si"})
    public void systemInfo() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        ansiConsole.yellow(">> OS\n");
        ansiConsole.cyan(" Arch: %s | OS: %s | Version: %s\n", os.getArch(), os.getName(), os.getVersion());
        ansiConsole.cyan(" Available processors: %d\n", os.getAvailableProcessors());
        ansiConsole.cyan(" Load avg: %f\n", os.getSystemLoadAverage());

        RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
        ansiConsole.yellow(">> Runtime\n");
        ansiConsole.cyan(" Uptime: %s\n", r.getUptime());
        ansiConsole.cyan(" VM name: %s | Vendor: %s | Version: %s\n", r.getVmName(), r.getVmVendor(), r.getVmVersion());

        ThreadMXBean t = ManagementFactory.getThreadMXBean();
        ansiConsole.yellow(">> Threads\n");
        ansiConsole.cyan(" Peak threads: %d\n", t.getPeakThreadCount());
        ansiConsole.cyan(" Live thread #: %d\n", t.getThreadCount());
        ansiConsole.cyan(" Total started threads: %d\n", t.getTotalStartedThreadCount());
        ansiConsole.cyan(" Current thread CPU time: %d\n", t.getCurrentThreadCpuTime());
        ansiConsole.cyan(" Current thread User time #: %d\n", t.getCurrentThreadUserTime());

        Arrays.stream(t.getAllThreadIds()).sequential().forEach(value -> {
            ansiConsole.cyan(" Thread (%d): %s %s\n", value,
                    t.getThreadInfo(value).getThreadName(),
                    t.getThreadInfo(value).getThreadState().toString()
            );
        });

        MemoryMXBean m = ManagementFactory.getMemoryMXBean();
        ansiConsole.yellow(">> Memory\n");
        ansiConsole.cyan(" Heap: %s\n", m.getHeapMemoryUsage().toString());
        ansiConsole.cyan(" Non-heap: %s\n", m.getNonHeapMemoryUsage().toString());
        ansiConsole.cyan(" Pending GC: %s\n", m.getObjectPendingFinalizationCount());
    }

    @ShellMethod(value = "Print thread pool metrics", key = {"pool-stats", "ps"})
    public void poolStats() {
        print("Task Executor Pool", threadPoolExecutor.getThreadPoolExecutor());
    }

    private void print(String pool, ThreadPoolExecutor tpe) {
        ansiConsole.yellow(">> %s:\n", pool);
        ansiConsole.cyan("running: %s\n", threadPoolExecutor.isRunning());
        ansiConsole.cyan("poolSize: %s\n", tpe.getPoolSize());
        ansiConsole.cyan("maximumPoolSize: %s\n", tpe.getMaximumPoolSize());
        ansiConsole.cyan("corePoolSize: %s\n", tpe.getCorePoolSize());
        ansiConsole.cyan("activeCount: %s\n", tpe.getActiveCount());
        ansiConsole.cyan("completedTaskCount: %s\n", tpe.getCompletedTaskCount());
        ansiConsole.cyan("taskCount: %s\n", tpe.getTaskCount());
        ansiConsole.cyan("largestPoolSize: %s\n", tpe.getLargestPoolSize());
    }
}

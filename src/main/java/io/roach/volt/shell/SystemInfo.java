package io.roach.volt.shell;

import io.roach.volt.shell.support.AnsiConsole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

@ShellComponent
@ShellCommandGroup(CommandGroups.ADMIN)
public class SystemInfo {
    @Autowired
    @Qualifier("threadPoolTaskExecutor")
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

    @ShellMethod(value = "Print thread pool stats", key = {"pool-stats", "ps"})
    public void poolStats() {
        ThreadPoolStats threadPoolStats = ThreadPoolStats.from(threadPoolExecutor.getThreadPoolExecutor());
        ansiConsole.yellow(">> Thread Pool Status:\n");
        ansiConsole.cyan("running: %s\n", threadPoolExecutor.isRunning());
        ansiConsole.cyan("poolSize: %s\n", threadPoolStats.poolSize);
        ansiConsole.cyan("maximumPoolSize: %s\n", threadPoolStats.maximumPoolSize);
        ansiConsole.cyan("corePoolSize: %s\n", threadPoolStats.corePoolSize);
        ansiConsole.cyan("activeCount: %s\n", threadPoolStats.activeCount);
        ansiConsole.cyan("completedTaskCount: %s\n", threadPoolStats.completedTaskCount);
        ansiConsole.cyan("taskCount: %s\n", threadPoolStats.taskCount);
        ansiConsole.cyan("largestPoolSize: %s\n", threadPoolStats.largestPoolSize);
    }

    static class ThreadPoolStats {
        public static ThreadPoolStats from(ThreadPoolExecutor pool) {
            ThreadPoolStats instance = new ThreadPoolStats();
            instance.corePoolSize = pool.getCorePoolSize();
            instance.poolSize = pool.getPoolSize();
            instance.maximumPoolSize = pool.getMaximumPoolSize();
            instance.activeCount = pool.getActiveCount();
            instance.taskCount = pool.getTaskCount();
            instance.largestPoolSize = pool.getLargestPoolSize();
            instance.completedTaskCount = pool.getCompletedTaskCount();
            return instance;
        }

        public int maximumPoolSize;

        public int poolSize;

        public int activeCount;

        public long corePoolSize;

        public long taskCount;

        public int largestPoolSize;

        public long completedTaskCount;
    }
}

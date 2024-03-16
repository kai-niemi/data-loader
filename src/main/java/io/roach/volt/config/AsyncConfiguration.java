package io.roach.volt.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.request.async.CallableProcessingInterceptor;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;

@Configuration
@EnableAspectJAutoProxy
@EnableAsync
public class AsyncConfiguration implements AsyncConfigurer {
    @Value("${application.maximum-threads}")
    private int threadPoolSize;

    /**
     * Executor used exclusively for @Async methods.
     */
    @Override
    public AsyncTaskExecutor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
//                threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors());
        executor.setMaxPoolSize(5);
//        executor.setQueueCapacity(5);

        executor.setThreadNamePrefix("async-");
        executor.setAwaitTerminationSeconds(5);
        executor.setStrictEarlyShutdown(true);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAcceptTasksAfterContextClose(false);
        executor.initialize();

        return executor;
    }

    @Bean(name = "threadPoolTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolSize > 0
                ? threadPoolSize : Runtime.getRuntime().availableProcessors());
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(100 / 4);

        executor.setThreadNamePrefix("pool-");
        executor.setAwaitTerminationSeconds(5);
        executor.setStrictEarlyShutdown(true);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAcceptTasksAfterContextClose(false);
        executor.initialize();

        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService pubSubExecutorService() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public CallableProcessingInterceptor callableProcessingInterceptor() {
        return new TimeoutCallableProcessingInterceptor();
    }
}


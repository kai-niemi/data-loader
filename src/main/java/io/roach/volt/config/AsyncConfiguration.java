package io.roach.volt.config;

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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        return asyncTaskExecutor();
    }

    @Bean(name = "asyncTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor asyncTaskExecutor() {
        int poolSize = threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize * 2);

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
        int poolSize = threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors();

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize * 2);

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


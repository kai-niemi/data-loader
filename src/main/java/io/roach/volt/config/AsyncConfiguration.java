package io.roach.volt.config;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.support.TaskUtils;
import org.springframework.web.context.request.async.CallableProcessingInterceptor;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;

@Configuration
@EnableAspectJAutoProxy
@EnableAsync
public class AsyncConfiguration implements AsyncConfigurer {
    @Value("${application.maximum-threads}")
    private int threadPoolSize;

    @Override
    public AsyncTaskExecutor getAsyncExecutor() {
        return asyncTaskExecutor();
    }

    /**
     * Executor for @Async processing and app event multicasting.
     */
    @Bean(name = "asyncTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor asyncTaskExecutor() {
        int poolSize = threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors() * 4;
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize * 2);
        executor.setThreadNamePrefix("volt-async-");
        executor.setStrictEarlyShutdown(true);
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.setAcceptTasksAfterContextClose(false);
        executor.initialize();
        return executor;
    }

    /**
     * Executor for pub/sub event dispatching.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService pubSubExecutorService() {
        return Executors.newCachedThreadPool();
    }

    @Bean("applicationEventMulticaster")
    public ApplicationEventMulticaster simpleApplicationEventMulticaster(
            @Autowired @Qualifier("asyncTaskExecutor") Executor executor) {
        SimpleApplicationEventMulticaster eventMulticaster = new SimpleApplicationEventMulticaster();
        eventMulticaster.setTaskExecutor(executor);
        eventMulticaster.setErrorHandler(TaskUtils.getDefaultErrorHandler(true));
        return eventMulticaster;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }

    @Bean
    public CallableProcessingInterceptor callableProcessingInterceptor() {
        return new TimeoutCallableProcessingInterceptor();
    }
}


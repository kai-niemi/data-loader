package io.roach.volt.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
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

import io.roach.volt.csv.model.ApplicationModel;

@Configuration
@EnableAspectJAutoProxy
@EnableAsync
public class AsyncConfiguration implements AsyncConfigurer {
    @Value("${application.maximum-threads}")
    private int threadPoolSize;

    @Autowired
    private ApplicationModel applicationModel;

    /**
     * Executor used exclusively for @Async methods.
     */
    @Override
    public AsyncTaskExecutor getAsyncExecutor() {
        return threadPoolTaskExecutor();
    }

    @Bean(name = "threadPoolTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        int poolSize = threadPoolSize > 0 ? threadPoolSize : Runtime.getRuntime().availableProcessors();

        // Adjust to at least as many threads as potentially concurrent producers (# of tables)
        poolSize = Math.max(applicationModel.getTables().size(), poolSize);

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize * 2);
        executor.setThreadNamePrefix("volt-");
        executor.setAwaitTerminationSeconds(5);

//        executor.setStrictEarlyShutdown(true);
//        executor.setWaitForTasksToCompleteOnShutdown(false);
//        executor.setAcceptTasksAfterContextClose(false);

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


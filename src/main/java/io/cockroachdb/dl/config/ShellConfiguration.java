package io.cockroachdb.dl.config;

import io.cockroachdb.dl.shell.support.AnotherFileValueProvider;
import io.cockroachdb.dl.shell.support.DirectoryValueProvider;
import io.cockroachdb.dl.shell.support.FunctionValueProvider;
import io.cockroachdb.dl.shell.support.TableNameProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ShellConfiguration {
    @Bean
    public FunctionValueProvider functionValueProvider() {
        return new FunctionValueProvider();
    }

    @Bean
    public TableNameProvider tableNameProvider() {
        return new TableNameProvider();
    }

    @Bean
    public DirectoryValueProvider pathValueProvider() {
        return new DirectoryValueProvider();
    }

    @Bean
    public AnotherFileValueProvider anotherFileValueProvider() {
        return new AnotherFileValueProvider();
    }
}

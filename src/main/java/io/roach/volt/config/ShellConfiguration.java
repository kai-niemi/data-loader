package io.roach.volt.config;

import io.roach.volt.shell.support.AnotherFileValueProvider;
import io.roach.volt.shell.support.DirectoryValueProvider;
import io.roach.volt.shell.support.FunctionValueProvider;
import io.roach.volt.shell.support.TableNameProvider;
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

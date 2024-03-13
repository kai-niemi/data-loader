package io.roach.volt.shell.support;

import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.ExpressionRegistryBuilder;
import io.roach.volt.expression.FunctionDef;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import java.util.ArrayList;
import java.util.List;

public class FunctionValueProvider implements ValueProvider {
    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        List<CompletionProposal> result = new ArrayList<>();

        final ExpressionRegistry registry = ExpressionRegistryBuilder.build(null);

        String prefix = completionContext.currentWordUpToCursor();
        if (prefix == null) {
            prefix = "";
        }

        for (FunctionDef functionDef : registry.functionDefinitions()) {
            if (functionDef.idMatchesPrefix(prefix)) {
                result.add(new CompletionProposal(functionDef.getId())
                                .value("\"" + functionDef.getId() + "()\"")
                        .category(functionDef.getCategory())
                        .description(functionDef.getDescription())
                        .displayText(functionDef.getId()));
            }
        }
        registry.variableNames().forEach(fn -> {
            result.add(new CompletionProposal(fn)
                    .category("constant"));
        });

        return result;
    }
}


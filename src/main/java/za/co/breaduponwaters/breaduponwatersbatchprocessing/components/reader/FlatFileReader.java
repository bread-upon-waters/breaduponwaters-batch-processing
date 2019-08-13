package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.reader;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ResolvableType;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

@StepScope
@Component
public class FlatFileReader implements ItemReader<Transaction> {

    @Value("#{stepExecutionContext['file']}")
    private Resource resource;

    public Transaction read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("flatFileItemReader")
                .resource(resource)
                .delimited()
                .names(new String[]{})
                .fieldSetMapper(fieldSet -> {
                    return Transaction.builder()
                            .account(fieldSet.readString("account"))
                            .amount(fieldSet.readBigDecimal("amount"))
                            .timestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"))
                            .build();
                })
                .build()
                .read();
    }
}

package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.pattitioning;

import lombok.AllArgsConstructor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep.TransactionItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

import javax.sql.DataSource;

@AllArgsConstructor
@Profile(value = "!master")
@Configuration
public class RemotePartitioningWorkerBatchConfiguration {

    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;

    @Bean
    public Step workerStep() {
        return workerStepBuilderFactory.get("workerStep")
                .outputChannel(replies())
                .inputChannel(requests())
                .<Transaction, Transaction>chunk(100)
                .reader(workerReaderCSV(null))
                .processor(workerProcessor())
                .writer(workerWriter(null))
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Transaction> workerReaderCSV(@Value("#{jobParameters['inputCSVFile']}") Resource resource) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("csvFileItemReader")
                .resource(resource)
                .delimited()
                .names(new String[]{"account", "amount", "timestamp"})
                .fieldSetMapper(fieldSet -> {
                    return Transaction.builder()
                            .account(fieldSet.readString("account"))
                            .amount(fieldSet.readBigDecimal("amount"))
                            .timestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"))
                            .build();
                })
                .build();
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<Transaction> workerWriter(@Qualifier("domainDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<Transaction, Transaction> workerProcessor() {
        return new TransactionItemProcessor();
    }

    @Bean
    public MessageChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(requests())
                .get();
    }

    @Bean
    public MessageChannel replies() {
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(replies())
                .handle(Amqp.outboundAdapter(amqpTemplate)
                        .routingKey("replies")
                )
                .get();
    }
}

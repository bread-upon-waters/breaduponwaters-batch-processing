package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
@Configuration
public class AsyncProcessorStepBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobLauncher jobLauncher;
    private final ApplicationConfig config;

    @Bean
    public IntegrationFlow inboundFlowAsyncProcessor(){
        return IntegrationFlows.from(asyncProcessorRequest())
                .handle(new JobLaunchingMessageHandler(jobLauncher){
                    @Override
                    public JobExecution launch(JobLaunchRequest request) throws JobExecutionException {
                        return super.launch(request);
                    }
                })
                .channel(asyncProcessorReplies())
                .get();
    }

    @Bean
    public DirectChannel asyncProcessorReplies(){
        return MessageChannels.direct().get();
    }

    @Bean
    public DirectChannel asyncProcessorRequest(){
        return MessageChannels.direct().get();
    }

    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws Exception {
        AtomicBoolean enabled = new AtomicBoolean(config.getAsyncProcessorEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputCSVFile","classpath:/data/csv/transactionsMultithread.csv")
                            .toJobParameters();
            jobLauncher.run(asyncProcessorStepJob(null), jobParameters);
        }
    }

    //
    @Bean
    public Job asyncProcessorStepJob(JobBuilderFactory masterJobBuilderFactory) throws Exception {
        return jobBuilderFactory.get("asyncProcessorStepJob")
                .incrementer(new RunIdIncrementer())
                .start(asyncProcessorStep())
                .build();
    }
    //

    //
    @Bean
    public Step asyncProcessorStep() throws Exception {
        return stepBuilderFactory.get("asyncProcessorStep")
                .<Transaction, Transaction>chunk(100)
                .reader(transactionReader(null))
                .processor(asyncProcessor())
                //.processor(highInputOutputItemProcessor()) // without Async
                 .writer(asyncWriter())
                //.writer(transactionWriter(null)) //without Async
                .build();
        //
    }

    @Bean
    public TaskExecutor asyncProcessorTaskExecutor() {
        return new ThreadPoolTaskExecutor(){
            {
                setThreadGroupName("async_processor-");
                setCorePoolSize(4);
                setMaxPoolSize(4);
                afterPropertiesSet();
            }
        };
    }

    @Bean
    public AsyncItemWriter asyncWriter() throws Exception {
        AsyncItemWriter<Transaction> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(transactionWriter(null));
        asyncItemWriter.afterPropertiesSet();
        return asyncItemWriter;
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<Transaction> transactionWriter(@Qualifier("domainDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public AsyncItemProcessor asyncProcessor() throws Exception {
        AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(new HighInputOutputItemProcessor());
        asyncItemProcessor.setTaskExecutor(asyncProcessorTaskExecutor());
        asyncItemProcessor.afterPropertiesSet();
        return asyncItemProcessor;
    }

    @StepScope
    @Bean
    public ItemProcessor<Transaction, Transaction> highInputOutputItemProcessor() throws Exception {
        return new HighInputOutputItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<? extends Transaction> transactionReader(@Value("#{jobParameters['inputCSVFile']}")Resource resource) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("CSVFileItemReader")
                .resource(resource)
                .delimited()
                .names(new String []{"account", "amount", "timestamp"})
                .fieldSetMapper(fieldSet -> {
                    return Transaction.builder()
                            .account(fieldSet.readString("account"))
                            .amount(fieldSet.readBigDecimal("amount"))
                            .timestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"))
                            .build();
                })
                .build();
    }
}

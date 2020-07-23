package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
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
public class MultiThreadStepBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobLauncher jobLauncher;
    private final ApplicationConfig config;

    @Bean
    public IntegrationFlow inboundFlowMultiThreaded(){
        return IntegrationFlows.from(multiThreadRequest())
                .handle(new JobLaunchingMessageHandler(jobLauncher){
                    @Override
                    public JobExecution launch(JobLaunchRequest request) throws JobExecutionException {
                        return super.launch(request);
                    }
                })
                .channel(multiThreadReplies())
                .get();
    }

    @Bean
    public DirectChannel multiThreadReplies(){
        return MessageChannels.direct().get();
    }

    @Bean
    public DirectChannel multiThreadRequest(){
        return MessageChannels.direct().get();
    }

    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        AtomicBoolean enabled = new AtomicBoolean(config.getMultiThreadEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputCSVFile","classpath:/data/csv/transactionsMultithread.csv")
                            .toJobParameters();
            jobLauncher.run(multiThreadedStepJob(null), jobParameters);
        }
    }

    //
    @Bean
    public Job multiThreadedStepJob(JobBuilderFactory masterJobBuilderFactory){
        return jobBuilderFactory.get("multiThreadedStepJob")
                .incrementer(new RunIdIncrementer())
                .start(multiThreadedStep())
                //.listener((JobExecutionListener) jobExecutionListener)
                .build();
    }
    //

    //
    @Bean
    public Step multiThreadedStep(){
        return stepBuilderFactory.get("multiThreadedStep")
                .<Transaction, Transaction>chunk(100)
                .reader(reader(null))
                .processor(processor())
                 .writer(writer(null))
                //.listener((ChunkListener) batchChunkListener)
                .taskExecutor(taskExecutor()) // MultiThreaded Step
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor(){
            {
                setThreadGroupName("multithread-taskexecutor");
                setCorePoolSize(4);
                setMaxPoolSize(4);
                //setQueueCapacity(10);
                //setThreadNamePrefix("Xxxx-");
                afterPropertiesSet();
            }
        };
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<? super Transaction> writer(@Qualifier("domainDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<? super Transaction,? extends Transaction> processor() {
        return new TransactionItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<? extends Transaction> reader(@Value("#{jobParameters['inputCSVFile']}")Resource resource) {
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

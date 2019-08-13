package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.asyncprocessorwriterstep;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.annotation.Scheduled;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadingstep.EnhanceItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@AllArgsConstructor
@Configuration
public class AsyncProcessorWriterStepBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;
    private ApplicationConfig config;


    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        AtomicBoolean enabled = new AtomicBoolean(config.getAsyncProcessorEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputCSVFile", "classpath:/data/csv/transactionsMultithread.csv")
                            .toJobParameters();
            jobLauncher.run(asyncProcessorWriterStepJob(null), jobParameters);
        }
    }

    //
    @Bean
    public Step asyncProcessorWriterStep() {
        return stepBuilderFactory.get("asyncProcessorWriterStep")
                .<Transaction, Transaction>chunk(100)
                .reader(asyncProcessorWriterReaderCSV(null))
                .processor((ItemProcessor) asyncItemProcessor())
                 .writer(asyncProcessorWriterWriter(null))
                //.taskExecutor(taskExecutor) // MultiThread Step
                .build();
    }

    @StepScope
    @Bean
    public AsyncItemWriter<Transaction> asyncItemWriter() {
        AsyncItemWriter<Transaction> asyncItemWriter = new AsyncItemWriter<Transaction>();
        asyncItemWriter.setDelegate(asyncProcessorWriterWriter(null));
        return asyncItemWriter;
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<Transaction> asyncProcessorWriterWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public AsyncItemProcessor<Transaction,Transaction> asyncItemProcessor() {
        AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor = new AsyncItemProcessor<Transaction, Transaction>();
        asyncItemProcessor.setDelegate(new EnhanceItemProcessor());
        asyncItemProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return asyncItemProcessor;
    }

    @StepScope
    @Bean
    public FlatFileItemReader<? extends Transaction> asyncProcessorWriterReaderCSV(@Value("#{jobParameters['inputCSVFile']}")Resource resource) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("csvFileItemReader")
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

    @Bean
    public Job asyncProcessorWriterStepJob(JobBuilderFactory masterJobBuilderFactory) {
        return jobBuilderFactory.get("asyncProcessorWriterStepJob")
                .start(asyncProcessorWriterStep())
                .build();
    }
    //
}

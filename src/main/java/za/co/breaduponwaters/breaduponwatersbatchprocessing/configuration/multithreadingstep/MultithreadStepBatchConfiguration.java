package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadingstep;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
@Configuration
public class MultithreadStepBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;
    private ApplicationConfig config;

    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        AtomicBoolean enabled = new AtomicBoolean(config.getMultiThreadEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputCSVFile","classpath:/data/csv/transactionsMultithread.csv")
                            .toJobParameters();
            jobLauncher.run(multiThreadStepJob(null), jobParameters);
        }
    }

    //
    @Bean
    public Step multiThreadStep(){
        return stepBuilderFactory.get("multiTheadStep")
                .<Transaction, Transaction>chunk(100)
                .reader(reader(null))
                .processor(processor())
                 .writer(writer(null))
                .taskExecutor(taskExecutor()) // MultiThread Step
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor(){
            {
                setThreadGroupName("multithread-step");
                setCorePoolSize(4);
                setMaxPoolSize(4);
                afterPropertiesSet();
            }
        };
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<? super Transaction> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<? super Transaction,? extends Transaction> processor() {
        return new EnhanceItemProcessor();
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

    @Bean
    public Job multiThreadStepJob(JobBuilderFactory masterJobBuilderFactory){
        return jobBuilderFactory.get("multiTheadStepJob")
                .start(multiThreadStep())
                .build();
    }
    //
}

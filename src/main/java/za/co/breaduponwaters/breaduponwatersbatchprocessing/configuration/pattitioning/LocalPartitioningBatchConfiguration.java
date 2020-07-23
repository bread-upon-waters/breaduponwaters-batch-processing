package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.pattitioning;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep.TransactionItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
@Configuration
public class LocalPartitioningBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private JobLauncher jobLauncher;
    private ApplicationConfig config;

    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        AtomicBoolean enabled = new AtomicBoolean(config.getRemotePartitionEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputFiles","classpath:/data/transaction/transaction*.csv")
                            .toJobParameters();
            jobLauncher.run(localPartitioningJob(), jobParameters);
        }
    }

    @Bean
    public Job localPartitioningJob(){
        return jobBuilderFactory.get("localPartitioningJob")
                .incrementer(new RunIdIncrementer())
                .flow(masterSteplc())
                .end()
                .build();
    }

    @Bean
    public Step masterSteplc(){
        return stepBuilderFactory.get("masterStep")
                //.partitioner(workerStep())
                .partitioner("workerSteplc", partitionerlc(null))
                .gridSize(10) // gridSize is ignore with file resource partitioning
                .taskExecutor(localPartitioningTaskExecutor())
                .build();
    }

    @Bean
    public Step workerSteplc() {
        return stepBuilderFactory.get("workerStep")
                .<Transaction, Transaction>chunk(1)
                .reader(workerReaderCSVlc(null))
                .processor(workerProcessorlc())
                .writer(workerWriterlc(null))
                .build();
    }

    @Bean
    @StepScope
    public MultiResourcePartitioner partitionerlc(@Value("#{jobParameters['inputFiles']}")Resource[] resources){
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setKeyName("files");
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public TaskExecutor localPartitioningTaskExecutor() {
        return new ThreadPoolTaskExecutor(){
            {
                setThreadGroupName("local_partitioning-");
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
    public FlatFileItemReader<Transaction> workerReaderCSVlc(@Value("#{jobParameters['inputCSVFile']}") Resource resource) {
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
    public JdbcBatchItemWriter<Transaction> workerWriterlc(@Qualifier("domainDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<Transaction, Transaction> workerProcessorlc() {
        return new TransactionItemProcessor();
    }

}

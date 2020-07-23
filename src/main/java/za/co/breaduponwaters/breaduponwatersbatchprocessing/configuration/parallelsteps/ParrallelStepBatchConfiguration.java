package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.parallelsteps;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.NoTypePermission;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.scheduling.annotation.Scheduled;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep.TransactionItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.utils.DateConverterAdapter;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
@Configuration
public class ParrallelStepBatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;
    private ApplicationConfig config;

    @Scheduled(cron = "${batch.job.cron}")
    public void schedule() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        AtomicBoolean enabled = new AtomicBoolean(config.getParallelEnabled());
        if (enabled.get()) {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addString("inputCSVFile","classpath:/data/csv/transactionsMultithread.csv")
                            .addString("inputXMLFile","classpath:/data/xml/transactionsMultithread.xml")
                            .toJobParameters();
            jobLauncher.run(parallelStepJob(null), jobParameters);
        }
    }

    //
    @Bean
    public Job parallelStepJob(JobBuilderFactory masterJobBuilderFactory){
        return jobBuilderFactory.get("parallelStepJob")
                .incrementer(new RunIdIncrementer())
                .start(splitFlow())
                //.next(parrallelStepStep4())
                .end()
                .build();
    }

    @Bean
    public Flow splitFlow(){
        Flow flow1 = new FlowBuilder<Flow>("flow2")
                .start(parrallelStepStep1())
                .build();
        Flow flow2 = new FlowBuilder<Flow>("flow2")
                .start(parrallelStepStep2())
                //.next(parrallelStepStep3())
                .build();

        return  new FlowBuilder<Flow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor("Spring_batch-"))
                .add(flow1, flow2)
                .build();
    }

    @Bean
    public Step parrallelStepStep1(){
        return stepBuilderFactory.get("parallelStep1")
                .<Transaction, Transaction>chunk(100)
                .reader(parallelStepReaderCSV(null))
                .processor(parallelStepProcessor())
                 .writer(parrallelStepWriter(null))
                //.taskExecutor(taskExecutor) // MultiThread Step
                .build();
    }

    @Bean
    public Step parrallelStepStep2(){
        return stepBuilderFactory.get("parallelStep2")
                .<Transaction, Transaction>chunk(100)
                .reader(parrallelStepReaderXML(null))
                .processor(parallelStepProcessor())
                .writer(parrallelStepWriter(null))
                //.taskExecutor(taskExecutor) // MultiThread Step
                .build();
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<? super Transaction> parrallelStepWriter(@Qualifier("domainDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<? super Transaction,? extends Transaction> parallelStepProcessor() {
        return new TransactionItemProcessor();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<? extends Transaction> parallelStepReaderCSV(@Value("#{jobParameters['inputCSVFile']}")Resource resource) {
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

    @StepScope
    @Bean
    public StaxEventItemReader<? extends Transaction> parrallelStepReaderXML(@Value("#{jobParameters['inputXMLFile']}")Resource resource) {
        return new StaxEventItemReaderBuilder<Transaction>()
                .name("xmlFileItemReader")
                .resource(resource)
                .unmarshaller(xStreamMarshaller())
                .addFragmentRootElements("transaction")
                .build();
    }

    @Bean
    public XStreamMarshaller xStreamMarshaller() {
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("transaction", Transaction.class);
        Class[] classes = new Class[]{
                Transaction.class
        };

        return new XStreamMarshaller(){
            @Override
            protected void customizeXStream(XStream xstream) {
                setAliases(aliases);
                xstream.registerConverter(new DateConverterAdapter());
                xstream.addPermission(NoTypePermission.NONE);
                xstream.allowTypes(classes);
                super.customizeXStream(xstream);
            }
        };
    }
    //
}

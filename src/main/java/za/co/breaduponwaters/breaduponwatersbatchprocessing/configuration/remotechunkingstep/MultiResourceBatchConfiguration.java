package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.remotechunkingstep;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.retry.annotation.CircuitBreaker;
import org.springframework.retry.annotation.Recover;

import javax.sql.DataSource;

@AllArgsConstructor
//@Configuration
public class MultiResourceBatchConfiguration {
    private ResourcePatternResolver resourcePatternResolver;
    private StepBuilderFactory stepBuilderFactory;
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public JdbcBatchItemWriter<String> multiResourceWriter(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO XXXXX VALUES ()")
                .build();
    }
    
    @StepScope
    @Bean
    public MultiResourceItemReader multiResourceReader(@Value("#{jobExecutionContext['localFiles']}") String path){
        MultiResourceItemReader resources = new MultiResourceItemReader();
        resources.setName("");
        resources.setDelegate(delegate());
        return null;
    }

    @Bean
    public FlatFileItemReader<String> delegate() {
        return new FlatFileItemReaderBuilder<String>()
                .name("fileReader")
                .delimited()
                .names(new String []{})
                .targetType(String.class)
                .build();
    }

    @Bean
    public Step load(){
        return stepBuilderFactory.get("load")
                .<String, String> chunk(100)
                .reader(multiResourceReader(null))
                .processor(multiResourceProcessor())
                .writer(multiResourceWriter(null))
                .build();
    }

    @Bean
    public Job multiResourceJob(JobExecutionListener jobExecutionListener){
        return jobBuilderFactory.get("s3jdbc")
                .listener(jobExecutionListener)
                .start(load())
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor multiResourceProcessor(){
        return new EnrichmentProcessor();
    }


    public class EnrichmentProcessor implements ItemProcessor<String, String>{

        @Recover
        public String fallback(String s){
            return "Whoops! something went wrong.";
        }

        @CircuitBreaker
        @Override
        public String process(String s) throws Exception {
            return null;
        }

        //@Autowired private RestTemplate restTemplate;
    }
}

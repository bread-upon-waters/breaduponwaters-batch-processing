package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.pattitioning;

import lombok.AllArgsConstructor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.integration.partition.RemotePartitioningMasterStepBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningMasterStepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.scheduling.annotation.Scheduled;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.ApplicationConfig;

import javax.validation.Valid;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
@Profile(value = "master")
@Configuration
public class MasterBatchConfiguration {

    private final RemotePartitioningMasterStepBuilderFactory masterStepBuilderFactory;

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
            jobLauncher.run(remotePartitionerJob(null), jobParameters);
        }
    }

    @Bean
    public MessageChannel requests(){
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate){
        return IntegrationFlows.from(requests())
                .handle(Amqp.outboundAdapter(amqpTemplate)
                        .routingKey("requests")
                )
                .get();
    }

    @Bean
    public MessageChannel replies(){
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory){
        return IntegrationFlows.from(Amqp.inboundAdapter((org.springframework.amqp.rabbit.connection.ConnectionFactory) connectionFactory, "replies"))
                .channel(replies())
                .get();
    }

    @Bean
    @StepScope
    public MultiResourcePartitioner partitioner(@Value("#{jobParameters['inputFiles']}")Resource[] resources){
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setKeyName("files");
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public Step masterStep(){
        return masterStepBuilderFactory.get("masterStep")
                .partitioner("workerStep", partitioner(null))
                .outputChannel(requests())
                .inputChannel(replies())
                .build();
    }

    @Bean
    public Job remotePartitionerJob(JobBuilderFactory masterJobBuilderFactory){
        return masterJobBuilderFactory.get("remotePartitionerJob")
                .start(masterStep())
                .build();
    }
}

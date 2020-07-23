package za.co.breaduponwaters.breaduponwatersbatchprocessing.controllers;

import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.web.bind.annotation.*;
import org.springframework.batch.core.launch.JobLauncher;

@RequestMapping("/batch")
@RestController
public class SpringBatchController {

    @Autowired
    private Job multiThreadedStepJob;
    @Autowired
    private Job asyncProcessorStepJob;
    @Autowired
    private DirectChannel asyncProcessorRequest;
    @Autowired
    private DirectChannel multiThreadRequest;
    @Autowired
    private DirectChannel multiThreadReplies;
    @Autowired
    private DirectChannel asyncProcessorReplies;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void launch(@RequestParam("jobName") String jobName){
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobName", jobName)
                .addString("inputCSVFile","classpath:/data/csv/transactionsMultithread.csv")
                .toJobParameters();
        if("multiThreadedStepJob".equalsIgnoreCase(jobName)) {
            JobLaunchRequest jobLaunchRequest = new JobLaunchRequest(multiThreadedStepJob, jobParameters);
            multiThreadReplies.subscribe(new MessageHandler() {
                @Override
                public void handleMessage(Message<?> message) throws MessagingException {
                    JobExecution jobExecution = (JobExecution) message.getPayload();
                    System.out.println(">> " + jobExecution.getJobInstance().getJobName() + " resulted in " + jobExecution.getStatus());
                }
            });
            multiThreadRequest.send(MessageBuilder
                    .withPayload(jobLaunchRequest)
                    .setReplyChannel(multiThreadReplies).build());
        }
        if("asyncProcessorStepJob".equalsIgnoreCase(jobName)){
            JobLaunchRequest jobLaunchRequest = new JobLaunchRequest(asyncProcessorStepJob, jobParameters);
            asyncProcessorReplies.subscribe(new MessageHandler() {
                @Override
                public void handleMessage(Message<?> message) throws MessagingException {
                    JobExecution jobExecution = (JobExecution) message.getPayload();
                    System.out.println(">> " + jobExecution.getJobInstance().getJobName() + " resulted in " + jobExecution.getStatus());
                }
            });
            asyncProcessorRequest.send(MessageBuilder
                    .withPayload(jobLaunchRequest)
                    .setReplyChannel(asyncProcessorReplies).build());
        }
    }
}
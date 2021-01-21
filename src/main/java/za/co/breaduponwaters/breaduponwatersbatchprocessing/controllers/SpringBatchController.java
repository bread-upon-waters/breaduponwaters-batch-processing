package za.co.breaduponwaters.breaduponwatersbatchprocessing.controllers;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
import org.springframework.web.client.HttpClientErrorException;

@RequestMapping("/batch")
@RestController
@Tag(name = "batch", description = "The batch processor API")
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

    @Operation(
            summary = "Trigger batch jobs by job name",
            description = "Start the batch job (e.g. multiThreadedStepJob, asyncProcessorStepJob, etc.)",
            tags = "batch"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Request successful",
                            content = @Content(mediaType = "application/json")
                    ),
                    @ApiResponse(
                            responseCode = "400",
                            description = "Bad Request",
                            content = @Content(mediaType = "application/json")
                    ),
                    @ApiResponse(
                            responseCode = "404",
                            description = "Job not found",
                            content = @Content(mediaType = "application/json")
                    ),

            }
    )
    @RequestMapping(value = "/", method = RequestMethod.POST)
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
        else if("asyncProcessorStepJob".equalsIgnoreCase(jobName)){
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
        else{
            throw new NotFound(jobName);
        }
    }


}

class NotFound extends RuntimeException {

    public NotFound(String message){
        super(message);
    }

}
package za.co.breaduponwaters.breaduponwatersbatchprocessing;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@EnableBatchProcessing
@EnableIntegration
@EnableBatchIntegration
@EnableScheduling
@SpringBootApplication
public class BreaduponwatersBatchProcessingApplication {

	public static void main(String[] args) {
			SpringApplication.run(BreaduponwatersBatchProcessingApplication.class, args);
	}

}

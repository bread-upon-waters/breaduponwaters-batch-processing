package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
//@EnableConfigurationProperties
@ConfigurationProperties(prefix = "batch.job")
public class ApplicationConfig {

    private String cron;
    private Boolean remotePartitionEnabled;
    private Boolean asyncProcessorEnabled;
    private Boolean multiThreadEnabled;
    private Boolean parallelEnabled;
}

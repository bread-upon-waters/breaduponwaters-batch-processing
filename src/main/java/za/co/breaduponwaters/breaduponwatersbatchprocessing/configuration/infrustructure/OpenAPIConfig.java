package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.infrustructure;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition()
public class OpenAPIConfig {

    @Bean
    public OpenAPI openAPI(){
        return new OpenAPI()
                .components(new Components())
                .info(getApiInfo());
    }

    private Info getApiInfo() {
        return new Info()
                .title("Spring Batch Processor Application")
                .description("Application for efficient and effective processing of bulk data with batch technology.")
                .version("1.0.0")
                .contact(new Contact().name("Bread Upon Waters").url("https://breaduponwaters.co.za").email("info@breaduponwaters.co.za"));
    }


}

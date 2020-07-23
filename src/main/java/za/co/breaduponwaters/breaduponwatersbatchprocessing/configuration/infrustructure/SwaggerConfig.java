package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.infrustructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Collections;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Bean
    public Docket apiDocket(){
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("za.co.breaduponwaters.breaduponwatersbatchprocessing"))
                .paths(PathSelectors.any())
                .build()
                .apiInfo(getApiInfo());
    }

    private ApiInfo getApiInfo() {
        return new ApiInfo(
                "Spring Batch Application",
                "Demonstrate Scaling & parallel processing with Spring Batch",
                "1.o.o",
                "",
                new Contact("Bread Upon Waters", "https://breaduponwaters.co.za", "info@breaduponwaters.co.za"),
                "",
                "",
                Collections.emptyList()
        );
    }

}

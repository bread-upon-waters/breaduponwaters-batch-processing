package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.infrustructure;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableJpaRepositories(
        entityManagerFactoryRef = "domainEntityManagerFactory",
        transactionManagerRef = "domainTransactionManager",
        basePackages = {
                "za.co.breaduponwaters.breaduponwatersbatchprocessing.repository.domain"
        }
)
public class DomainDataSourceConfiguration {

    @Bean("domainDataSourceProperties")
    public DataSourceProperties dataSourceProperties(){
        return new DataSourceProperties();
    }

    @Bean(name = "domainDataSource")
    @ConfigurationProperties(prefix = "spring.domain.datasource")
    public DataSource domainDatasource(@Qualifier("domainDataSourceProperties") DataSourceProperties dataSourceProperties){
        Map<String, String> properties = new HashMap<>();
        return dataSourceProperties.initializeDataSourceBuilder()
                .create()
                .build();
    }

    @Bean("domainEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(@Qualifier("domainDataSource") DataSource dataSource, EntityManagerFactoryBuilder builder) {
        //HashMap<String, Object> properties = new HashMap<>();
        //properties.put("spring.jpa.hibernate.ddl-auto",env.getProperty("spring.jpa.hibernate.ddl-auto"));
        //properties.put("spring.jpa.generate-ddl",env.getProperty("spring.jpa.generate-ddl"));
        //properties.put("spring.jpa.show-sql",env.getProperty("spring.jpa.show-sql"));
        //properties.put("spring.jpa.database-platform", env.getProperty("spring.jpa.database-platform"));
        return builder
                .dataSource(dataSource)
                //.properties(properties)
                .packages("za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain")
                .persistenceUnit("domain")
                .build();
    }

    @Bean("domainTransactionManager")
    public PlatformTransactionManager transactionManager(@Qualifier("domainEntityManagerFactory") EntityManagerFactory entityManagerFactory){
        return new JpaTransactionManager(entityManagerFactory);
    }
}

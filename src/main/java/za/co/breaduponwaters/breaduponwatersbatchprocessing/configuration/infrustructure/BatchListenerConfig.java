package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.infrustructure;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.gateway.GatewayProxyFactoryBean;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;


@Configuration
public class BatchListenerConfig implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean("events")
    public  DirectChannel events() {
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow inboundFlowListener(){
        return IntegrationFlows.from(events())
                .handle(CharacterStreamWritingMessageHandler.stdout())
                .get();
    }

    @Bean
    public Object jobExecutionListener(){
        GatewayProxyFactoryBean gatewayProxyFactoryBean = new GatewayProxyFactoryBean(JobExecutionListener.class);
        gatewayProxyFactoryBean.setDefaultRequestChannel(events());
        gatewayProxyFactoryBean.setBeanFactory(this.applicationContext);
        return gatewayProxyFactoryBean.getObject();
    }

    @Bean
    public Object chunkListener(){
        GatewayProxyFactoryBean gatewayProxyFactoryBean = new GatewayProxyFactoryBean(ChunkListener.class);
        gatewayProxyFactoryBean.setDefaultRequestChannel(events());
        gatewayProxyFactoryBean.setBeanFactory(this.applicationContext);
        return gatewayProxyFactoryBean.getObject();
    }
}

package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.writer;

import org.springframework.batch.item.*;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultCrudMethods;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.stereotype.Component;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.repository.domain.TransactionRepository;

import java.util.List;

@Component
public class TransactionRepositoryItemWriter implements ItemWriter<Transaction> {

    @Autowired
    TransactionRepository transactionRepository;


    @Bean
    public RepositoryMetadata repositoryMetadata() {
        return new DefaultRepositoryMetadata(TransactionRepository.class);
    }

    @Override
    public void write(List<? extends Transaction> list) throws Exception {
        DefaultCrudMethods defaultCrudMethods = new DefaultCrudMethods(repositoryMetadata());
        RepositoryItemWriterBuilder.RepositoryMethodReference<Transaction> repositoryMethodReference = new RepositoryItemWriterBuilder.RepositoryMethodReference<Transaction>(transactionRepository);
        new RepositoryItemWriterBuilder<Transaction>()
                .methodName(defaultCrudMethods.getSaveMethod().get().getName())
                .repository(repositoryMethodReference)
                .build()
                .write(list);
    }
}

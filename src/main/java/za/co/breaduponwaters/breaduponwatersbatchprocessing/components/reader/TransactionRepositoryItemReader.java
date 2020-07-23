package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.reader;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultCrudMethods;
import org.springframework.stereotype.Component;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.repository.domain.TransactionRepository;

@Component
public class TransactionRepositoryItemReader implements ItemReader<Transaction> {

    @Autowired
    TransactionRepository transactionRepository;
    @Autowired
    RepositoryMetadata repositoryMetadata;

    public Transaction read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        DefaultCrudMethods defaultCrudMethods = new DefaultCrudMethods(repositoryMetadata);
        RepositoryItemReaderBuilder.RepositoryMethodReference<Transaction> repositoryMethodReference = new RepositoryItemReaderBuilder.RepositoryMethodReference<Transaction>(transactionRepository);
        return new RepositoryItemReaderBuilder<Transaction>()
                .methodName(defaultCrudMethods.getFindAllMethod().get().getName())
                .repository(repositoryMethodReference)
                .build()
                .read();
    }
}

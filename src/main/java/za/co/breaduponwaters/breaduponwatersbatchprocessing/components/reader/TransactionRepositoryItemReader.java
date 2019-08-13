package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.reader;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultCrudMethods;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.stereotype.Component;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.repository.TransactionRepository;

import javax.sql.DataSource;

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

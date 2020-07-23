package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

@Slf4j
public class TransactionItemProcessor implements ItemProcessor<Transaction,Transaction> {

    @Override
    public Transaction process(Transaction transaction) throws Exception {
        log.info("Processing Item :: " + transaction);
        return transaction;
    }
}

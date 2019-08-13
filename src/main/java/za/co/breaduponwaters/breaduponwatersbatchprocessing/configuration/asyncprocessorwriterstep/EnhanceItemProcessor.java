package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.asyncprocessorwriterstep;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

@Slf4j
public class EnhanceItemProcessor implements ItemProcessor<Transaction,Transaction> {

    @Override
    public Transaction process(Transaction transaction) throws Exception {
        log.info("Processing Item :: " + transaction);
        Thread.sleep(5);
        return transaction;
    }
}

package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.AbstractEntity;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

@Slf4j
@Component
public class GenericItemProcessor implements ItemProcessor<Transaction, Transaction> {

    public Transaction process(Transaction transaction) throws Exception {
        log.info("Entity being processed :: %1" , transaction);
        return transaction;
    }
}

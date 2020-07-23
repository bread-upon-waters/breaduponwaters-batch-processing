package za.co.breaduponwaters.breaduponwatersbatchprocessing.configuration.multithreadedstep;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

import java.util.Random;

@Slf4j
public class HighInputOutputItemProcessor implements ItemProcessor<Transaction,Transaction> {

    @Override
    public Transaction process(Transaction transaction) throws Exception {
        Thread.sleep(new Random().nextInt(10));
        log.info("Processing Item is I/O intensive:: " + transaction);
        return transaction;
    }
}

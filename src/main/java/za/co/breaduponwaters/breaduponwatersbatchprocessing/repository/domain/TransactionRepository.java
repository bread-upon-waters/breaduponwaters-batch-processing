package za.co.breaduponwaters.breaduponwatersbatchprocessing.repository.domain;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

@Repository
public interface TransactionRepository extends PagingAndSortingRepository<Transaction, Long> {
}

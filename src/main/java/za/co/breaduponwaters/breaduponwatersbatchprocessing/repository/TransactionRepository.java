package za.co.breaduponwaters.breaduponwatersbatchprocessing.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

@Repository
public interface TransactionRepository extends PagingAndSortingRepository<Transaction, Long> {
}

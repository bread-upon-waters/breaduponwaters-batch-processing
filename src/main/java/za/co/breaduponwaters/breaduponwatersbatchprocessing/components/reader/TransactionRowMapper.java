package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.reader;

import org.springframework.jdbc.core.RowMapper;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.models.Transaction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionRowMapper implements RowMapper<Transaction> {

    @Override
    public Transaction mapRow(ResultSet resultSet, int i) throws SQLException {
        return Transaction.builder()
                .build();
    }
}

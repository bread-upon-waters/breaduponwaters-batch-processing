package za.co.breaduponwaters.breaduponwatersbatchprocessing.components.reader;

import org.springframework.batch.item.*;
import org.springframework.batch.item.database.StoredProcedureItemReader;
import org.springframework.batch.item.database.builder.StoredProcedureItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.SqlParameter;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain.Transaction;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class TransactionStoredProcedureItemReader implements ItemReader<Transaction> {

    @Autowired
    DataSource dataSource;
    @Value("#{jobParameters[clientName]}")
    String clientName;

    @Override
    public Transaction read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        SqlParameter [] parameters = {
                new SqlParameter("from_id", Types.VARCHAR),
                new SqlParameter("to_id", Types.NUMERIC)
        };
        PreparedStatementSetter stament = new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setString(1, clientName);
                ps.setString(2, clientName);

            }
        };
        StoredProcedureItemReader<Transaction> reader = new StoredProcedureItemReaderBuilder<Transaction>()
                .name("transaction_sp")
                .dataSource(dataSource)

                .procedureName("get_something")
                .rowMapper(new BeanPropertyRowMapper<Transaction>(Transaction.class))
                .parameters(parameters)
                .preparedStatementSetter(stament)
                .verifyCursorPosition(false)
                .build();

        reader.afterPropertiesSet();
        reader.open(new ExecutionContext());
        Transaction transaction = reader.read();
        reader.close();
        return transaction;
    }
}

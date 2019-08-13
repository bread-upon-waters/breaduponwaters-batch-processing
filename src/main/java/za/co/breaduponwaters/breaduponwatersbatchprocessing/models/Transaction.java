package za.co.breaduponwaters.breaduponwatersbatchprocessing.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import za.co.breaduponwaters.breaduponwatersbatchprocessing.utils.DateConverterAdapter;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
//@SequenceGenerator(name = AbstractEntity.GENERATOR, sequenceName = "seq_trn")
//@AttributeOverride(name = "id", column = @Column(name = "trn_id"))
@XmlRootElement( name = "data/transaction")
public class Transaction /**extends AbstractEntity<Long>**/ {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String account;
    private BigDecimal amount;
    //@XmlJavaTypeAdapter(DateConverterAdapter.class)
    private Date timestamp;
}

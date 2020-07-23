package za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "TRANSACTION")
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

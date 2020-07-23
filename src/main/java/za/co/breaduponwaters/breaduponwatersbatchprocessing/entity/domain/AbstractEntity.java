package za.co.breaduponwaters.breaduponwatersbatchprocessing.entity.domain;

import javax.persistence.*;
import java.io.Serializable;

@MappedSuperclass
public abstract class AbstractEntity<PK extends Serializable> {

    public static final String GENERATOR = "custom_generator";
    public static final String SEQ_NAME = "custom_seq";

    @Id
    @GeneratedValue
    //@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = GENERATOR)
    //@SequenceGenerator(name=GENERATOR, sequenceName = "SEQ_NAME", initialValue = 1, allocationSize = 50)
    private PK id;


}

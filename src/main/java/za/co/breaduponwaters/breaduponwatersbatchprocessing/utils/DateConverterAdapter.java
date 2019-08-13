package za.co.breaduponwaters.breaduponwatersbatchprocessing.utils;

import com.thoughtworks.xstream.converters.basic.DateConverter;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateConverterAdapter extends DateConverter {

    public DateConverterAdapter(){
        super("yyyy-MM-dd HH:mm:ss", new String[]{"yyyy-MM-dd HH:mm:ss"}, new GregorianCalendar().getTimeZone());
    }

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public boolean canConvert(Class type) {
        return type.equals(Date.class) || type.equals(Timestamp.class);
    }

    @Override
    public String toString(Object obj) {
        return dateFormat.format((Date) obj);
    }
}

package com.duia.core;

import com.duia.core.KafkaStreamApplication.KafkaStreamApplicationStat;
import org.springframework.stereotype.Component;
import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@Component
public @interface KafkaStream {
    KafkaStreamApplicationStat stat() default KafkaStreamApplicationStat.START;
}

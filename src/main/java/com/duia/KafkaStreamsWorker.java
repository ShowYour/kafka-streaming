package com.duia;

//import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@MapperScan(basePackages = {"com.duia.dao"})
public class KafkaStreamsWorker {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsWorker.class, args);
	}

}

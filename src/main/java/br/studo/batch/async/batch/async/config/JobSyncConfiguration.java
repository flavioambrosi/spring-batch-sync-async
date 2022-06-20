package br.studo.batch.async.batch.async.config;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.processor.CashbackRequestItemProcessor;
import br.studo.batch.async.batch.async.processor.JobCompletionNotificationListener;
import br.studo.batch.async.batch.async.reader.ItemFileReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
@EnableScheduling
@Slf4j
public class JobSyncConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobCompletionNotificationListener jobCompletionNotificationListener;

    @Value("${file.input}")
    private String fileInput;


    @Autowired
    private DataSource dataSource;

    public ItemReader<CashbackRequest> reader() {
        return new ItemFileReader(fileInput);
    }

    public CashbackRequestItemProcessor processor() {
        return new CashbackRequestItemProcessor();
    }

    @Bean
//    public JdbcBatchItemWriter<CashbackRequest> writer(DataSource dataSource) {
//        return new JdbcBatchItemWriterBuilder<CashbackRequest>().itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
//                .sql("INSERT INTO cashback (phoneNumber, cardNumber, amount, currency, status) VALUES (:phoneNumber, :cardNumber, :amount, :currency, :status)")
//                .dataSource(dataSource)
//                .build();
//    }
    public FlatFileItemWriter<CashbackRequest> writer(DataSource dataSource) {

        return new FlatFileItemWriterBuilder<CashbackRequest>()
                .name("Writer")
                .append(false)
                .resource(new FileSystemResource("transactions.txt"))
                .lineAggregator(new DelimitedLineAggregator<CashbackRequest>() {
                    {
                        setDelimiter(";");
                        setFieldExtractor(new BeanWrapperFieldExtractor<CashbackRequest>() {
                            {
                                setNames(new String[]{"phoneNumber", "cardNumber", "amount", "currency", "status"});
                            }
                        });
                    }
                })
                .build();
    }


    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step1())
                .end()
                .build();
    }

    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<CashbackRequest, CashbackRequest> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer(dataSource))
                .build();
    }
}

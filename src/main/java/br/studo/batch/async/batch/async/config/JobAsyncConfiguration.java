package br.studo.batch.async.batch.async.config;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.processor.CashbackRequestItemProcessor;
import br.studo.batch.async.batch.async.processor.JobCompletionNotificationListener;
import br.studo.batch.async.batch.async.reader.ItemFileReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class JobAsyncConfiguration {

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


    public AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncProcessor() {
        AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(processor2());
        asyncItemProcessor.setTaskExecutor(taskExecutor());

        return asyncItemProcessor;
    }

    public CashbackRequestItemProcessor processor2() {
        return new CashbackRequestItemProcessor();
    }

    public AsyncItemWriter<CashbackRequest> asyncWriter() {
        AsyncItemWriter<CashbackRequest> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(writer2(dataSource));
        return asyncItemWriter;
    }
    @Bean
    public JdbcBatchItemWriter<CashbackRequest> writer2(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<CashbackRequest>().itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO cashback (phoneNumber, cardNumber, amount, currency, status) VALUES (:phoneNumber, :cardNumber, :amount, :currency, :status)")
                .dataSource(dataSource)
                .build();
    }


    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(2);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }



    public Job  asyncJob() {
        return jobBuilderFactory.get("Asynchronous Processing JOB")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow( asyncManagerStep())
                .end()
                .build();
    }

    public Step asyncManagerStep() {
        return stepBuilderFactory.get("Asynchronous Processing : Read -> Process -> Write ")
                .<CashbackRequest, Future<CashbackRequest>> chunk(10)
                .reader(reader())
                .processor(asyncProcessor())
                .writer(asyncWriter())
                .taskExecutor(taskExecutor())
                .build();
    }


}

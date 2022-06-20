package br.studo.batch.async.batch.async.config;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.processor.CashbackRequestItemProcessor;
import br.studo.batch.async.batch.async.processor.JobCompletionNotificationListener;
import br.studo.batch.async.batch.async.reader.ItemFileReader;
import br.studo.batch.async.batch.async.writer.CashBackWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.concurrent.Future;

@Component
public class JobAsyncConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobCompletionNotificationListener jobCompletionNotificationListener;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${file.input}")
    private String fileInput;


    public Job asyncJob() {
        return jobBuilderFactory
                .get("Asynchronous Processing JOB")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(asyncManagerStep())
                .end()
                .build();
    }

    public Step asyncManagerStep() {
        return stepBuilderFactory
                .get("Asynchronous Processing : Read -> Process -> Write ")
                .<CashbackRequest, Future<CashbackRequest>>chunk(50)
                .reader(syncReader())
                .processor(asyncProcessor())
                .writer(asyncWriter())
                .build();
    }

    public AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncProcessor() {
        AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor());
        asyncItemProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());

        return asyncItemProcessor;
    }

    public AsyncItemWriter<CashbackRequest> asyncWriter() {
        AsyncItemWriter<CashbackRequest> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter(jdbcTemplate));

        return asyncItemWriter;
    }

    public ItemProcessor<CashbackRequest, CashbackRequest> itemProcessor() {
        return new CashbackRequestItemProcessor();
    }

    public ItemReader<CashbackRequest> syncReader() {
        return new ItemFileReader(fileInput);
    }

    public ItemWriter<CashbackRequest> itemWriter(JdbcTemplate jdbcTemplate) {
        return new CashBackWriter(jdbcTemplate);
    }
}
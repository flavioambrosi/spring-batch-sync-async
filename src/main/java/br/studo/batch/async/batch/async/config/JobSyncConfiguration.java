package br.studo.batch.async.batch.async.config;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.processor.CashbackRequestItemProcessor;
import br.studo.batch.async.batch.async.processor.JobCompletionNotificationListener;
import br.studo.batch.async.batch.async.reader.ItemFileReader;
import br.studo.batch.async.batch.async.writer.CashBackWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
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
    private JdbcTemplate jdbcTemplate;

    public ItemReader<CashbackRequest> reader() {
        return new ItemFileReader(fileInput);
    }

    public CashbackRequestItemProcessor processor() {
        return new CashbackRequestItemProcessor();
    }

    public ItemWriter<CashbackRequest> itemWriter(JdbcTemplate jdbcTemplate) {
        return new CashBackWriter(jdbcTemplate);
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
                .writer(itemWriter(jdbcTemplate))
                .build();
    }
}

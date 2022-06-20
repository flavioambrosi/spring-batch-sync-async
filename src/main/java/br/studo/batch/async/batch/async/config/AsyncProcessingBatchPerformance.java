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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class AsyncProcessingBatchPerformance {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobCompletionNotificationListener jobCompletionNotificationListener;

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
                .reader(asyncReader())
                .processor(asyncProcessor2())
                .writer(asyncWriter())
//                .taskExecutor(taskExecutor2())
                .build();
    }

    public AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncProcessor2() {
        AsyncItemProcessor<CashbackRequest, CashbackRequest> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor());
        asyncItemProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());

        return asyncItemProcessor;
    }

    @Bean
    public AsyncItemWriter<CashbackRequest> asyncWriter() {
        AsyncItemWriter<CashbackRequest> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter());
        return asyncItemWriter;
    }

    @Bean
    public TaskExecutor taskExecutor2() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }

    public ItemProcessor<CashbackRequest, CashbackRequest> itemProcessor() {
        return new CashbackRequestItemProcessor();
    }

    public ItemReader<CashbackRequest> asyncReader() {

        return new ItemFileReader(fileInput);
    }

    public FlatFileItemWriter<CashbackRequest> itemWriter() {

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
}
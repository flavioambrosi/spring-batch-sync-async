package br.studo.batch.async.batch.async.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;

@Configuration
@EnableScheduling
@Slf4j
public class JobScheduler {

    @Autowired
    private JobSyncConfiguration jobSyncConfiguration;

    @Autowired
    private JobAsyncConfiguration jobAsyncConfiguration;

    @Autowired
    private AsyncProcessingBatchPerformance asyncProcessingBatchPerformance;

    @Autowired
    private JobLauncher jobLauncher;

    @Scheduled(cron = "*/30 * * * * *")
    public void perform() throws Exception {

        Date date = new Date();


        JobExecution jobExecution = jobLauncher.run(asyncProcessingBatchPerformance.asyncJob(), new JobParametersBuilder().addDate("launchDate", date)
                .toJobParameters());

//        JobExecution jobExecution = jobLauncher.run(jobSyncConfiguration.importUserJob(), new JobParametersBuilder().addDate("launchDate", date)
//                .toJobParameters());


        log.info("Job Started at :" + date);
        log.info("Job Ended at :" + new Date());
        log.info("Batch job ends with status as " + jobExecution.getStatus());
    }
}

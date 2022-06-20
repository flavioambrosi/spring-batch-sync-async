package br.studo.batch.async.batch.async.processor;

import br.studo.batch.async.batch.async.model.Account;
import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.util.ReadCashbackFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private final JdbcTemplate jdbcTemplate;

    @Value("${file.input}")
    private String fileInput;

    @Autowired
    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Start account");

        jdbcTemplate.update("DELETE from account");
        jdbcTemplate.update("DELETE from cashback");
        jdbcTemplate.update("INSERT into account (totalTransactions, totalCredit, balance) values (0,100000,100000)");
        ExecutionContext jobContext = jobExecution.getExecutionContext();

        List<CashbackRequest> cashbackRequestList = ReadCashbackFileUtil.readFile(fileInput);
        jobContext.put("CASHBACKS", cashbackRequestList);

        BigDecimal total = cashbackRequestList.stream()
                .map(cashback -> cashback.getAmount())
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        jdbcTemplate.update("UPDATE account set totalTransactions = totalTransactions + " +
                cashbackRequestList.size() + ", balance = balance - " + total);

        String query = "SELECT totalCredit, balance, totalTransactions FROM account";
        jdbcTemplate.query(query, (rs, row) -> new Account(rs.getBigDecimal(1), rs.getBigDecimal(2), rs.getInt(3)))
                .stream()
                .forEach(account -> log.info("ACCOUNT {}", account));
    }


    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            String query = "SELECT phoneNumber, cardNumber, amount, currency, status FROM cashback";

            List<CashbackRequest> results = jdbcTemplate.query(query, (rs, row) -> new CashbackRequest(rs.getString(1), rs.getString(2), rs.getBigDecimal(3), rs.getString(4), rs.getString(5)));

            BigDecimal totalFailed = results.stream()
                    .filter(cashback -> "FAILED".equals(cashback.getStatus()))
                    .map(cashback -> cashback.getAmount())
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            BigDecimal totalSuccess = results.stream()
                    .filter(cashback -> "SUCCESS".equals(cashback.getStatus()))
                    .map(cashback -> cashback.getAmount())
                    .reduce(BigDecimal.ZERO, BigDecimal::add);


            log.info("TOTAL TRANSACTIONS: {} -- TOTAL FAILED AMOUNT TRANSACTIONS: {} -- TOTAL SUCCESS AMOUNT TRANSACTIONS: {}", results.size(), totalFailed, totalSuccess);

            query = "SELECT totalCredit, balance, totalTransactions FROM account";
            jdbcTemplate.query(query, (rs, row) -> new Account(rs.getBigDecimal(1), rs.getBigDecimal(2), rs.getInt(3)))
                    .stream()
                    .forEach(account -> log.info("ANTES - ACCOUNT {}", account));

            jdbcTemplate.update("UPDATE account set balance = balance + " + totalFailed);

            jdbcTemplate.query(query, (rs, row) -> new Account(rs.getBigDecimal(1), rs.getBigDecimal(2), rs.getInt(3)))
                    .stream()
                    .forEach(account -> log.info("DEPOIS - ACCOUNT {}", account));

        }
    }
}

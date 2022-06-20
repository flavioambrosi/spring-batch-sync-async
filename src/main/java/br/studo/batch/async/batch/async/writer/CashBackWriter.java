package br.studo.batch.async.batch.async.writer;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.io.Serializable;
import java.util.List;

public class CashBackWriter implements ItemWriter<CashbackRequest> {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public CashBackWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

        @Override
        public void write(List<? extends CashbackRequest> list) throws Exception {
            list.stream()
                    .forEach(cashbackRequest -> {
                        String sql = "INSERT INTO cashback (phoneNumber, cardNumber, amount, currency, status) VALUES (?, ?, ?, ?, ?)";
                        jdbcTemplate.update(sql,
                                cashbackRequest.getPhoneNumber(),
                                cashbackRequest.getCardNumber(),
                                cashbackRequest.getAmount(),
                                cashbackRequest.getCurrency(),
                                cashbackRequest.getStatus());
                    });
        }
    }

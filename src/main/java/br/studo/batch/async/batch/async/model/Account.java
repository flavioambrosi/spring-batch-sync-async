package br.studo.batch.async.batch.async.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Account {

    BigDecimal totalCredit;
    BigDecimal balance;
    Integer totalTransactions;
}

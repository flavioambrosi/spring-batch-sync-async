package br.studo.batch.async.batch.async.model;

import lombok.*;

import java.math.BigDecimal;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CashbackRequest {

    private String phoneNumber;
    private String cardNumber;
    private BigDecimal amount;
    private String currency;
    private String status;
}

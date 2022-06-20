package br.studo.batch.async.batch.async.processor;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;

@Slf4j
public class CashbackRequestItemProcessor implements ItemProcessor<CashbackRequest, CashbackRequest> {

    @Override
    public CashbackRequest process(CashbackRequest cashbackRequest) throws Exception {
        Date startDate = new Date();
        Thread.sleep(800);
        String phoneNumber = cashbackRequest.getPhoneNumber();
        String cardNumber = cashbackRequest.getCardNumber();
        String currency = cashbackRequest.getCurrency();
        BigDecimal amount = cashbackRequest.getAmount();

        Random random = new Random();
        boolean isSuccess = random.nextBoolean();

        String status = isSuccess ? "SUCCESS" : "FAILED";
        CashbackRequest transformedCashBack = new CashbackRequest(phoneNumber, cardNumber, amount, currency, status);

        Date endDate = new Date();
        long diffInMillies = startDate.getTime() - endDate.getTime();
        log.info("[{} millies] Converting ( {} ) into ( {} )", diffInMillies, cashbackRequest, transformedCashBack);

        return transformedCashBack;
    }
}

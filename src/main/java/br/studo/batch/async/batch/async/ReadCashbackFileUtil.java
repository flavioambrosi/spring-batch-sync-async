package br.studo.batch.async.batch.async;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ReadCashbackFileUtil {

    public static List<CashbackRequest> readFile(String fileInput) {
        try {
            InputStream is = ReadCashbackFileUtil.class.getClassLoader().getResourceAsStream(fileInput);
            Reader targetReader = new InputStreamReader(is);

            CSVReader csvReader = new CSVReaderBuilder(targetReader).build();
            return csvReader.readAll().stream().map(data -> {
                return CashbackRequest.builder()
                        .phoneNumber(data[0])
                        .cardNumber(data[1])
                        .amount(new BigDecimal(data[2]))
                        .currency(data[3])
                        .build();
            }).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Error parsing CSV", e);
        }
        return null;
    }
}

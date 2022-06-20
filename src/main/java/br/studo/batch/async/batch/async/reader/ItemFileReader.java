package br.studo.batch.async.batch.async.reader;

import br.studo.batch.async.batch.async.model.CashbackRequest;
import br.studo.batch.async.batch.async.util.ReadCashbackFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;

import java.util.List;

@Slf4j
public class ItemFileReader implements ItemReader<CashbackRequest> {

    private String fileInput;

    private List<CashbackRequest> cashbackRequestList;

    private int nextCashbackIndex;

    public ItemFileReader(String fileInput) {
        this.fileInput = fileInput;
    }

    @BeforeStep
    public void retrieveInterstepData(StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        this.cashbackRequestList = (List<CashbackRequest>) jobContext.get("CASHBACKS");
    }

    public CashbackRequest read() throws Exception {
        CashbackRequest request = null;
        if(this.cashbackRequestList == null){
            log.error("DEU ERRO NO LISTENER");
            this.cashbackRequestList = ReadCashbackFileUtil.readFile(fileInput);
        }

        if (this.cashbackRequestList != null) {
            if (nextCashbackIndex < cashbackRequestList.size()) {
                request = cashbackRequestList.get(nextCashbackIndex);
                log.info("Reading data : {}", request);
                nextCashbackIndex++;
            }
            else {
                nextCashbackIndex = 0;
            }
        }
        return request;
    }
}

package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import Dto.Salestransaction;

public class JsonUtil {
    private static final ObjectMapper obbjectMapper = new ObjectMapper();
    public static String convertTransactionToJson(Salestransaction transaction) {
        try {
            return obbjectMapper.writeValueAsString(transaction);
        }catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}

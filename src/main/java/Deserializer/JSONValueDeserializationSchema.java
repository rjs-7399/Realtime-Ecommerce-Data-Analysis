package Deserializer;
import Dto.Salestransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<Salestransaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Salestransaction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Salestransaction.class);
    }

    @Override
    public boolean isEndOfStream(Salestransaction salestransaction) {
        return false;
    }

    @Override
    public TypeInformation<Salestransaction> getProducedType() {
        return TypeInformation.of(Salestransaction.class);
    }
}

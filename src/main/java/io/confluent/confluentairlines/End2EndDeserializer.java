package io.confluent.confluentairlines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

public class End2EndDeserializer implements Deserializer<JsonNode> {

    private ObjectMapper objectMapper = new ObjectMapper();

    private String e2eAlgo;

    private String e2eSecret;

    public void setE2eAlgo(String e2eAlgo) {
        this.e2eAlgo = e2eAlgo;
    }

    public void setE2eSecret(String e2eSecret) {
        this.e2eSecret = e2eSecret;
    }


    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            byte[] keyValue = e2eSecret.getBytes();

            Key key = new SecretKeySpec(keyValue, e2eAlgo);
            Cipher c = Cipher.getInstance(e2eAlgo);
            c.init(Cipher.DECRYPT_MODE, key);
            byte[] bytes = c.doFinal(data);

            return objectMapper.readValue(new String(bytes), JsonNode.class);
        } catch (JsonProcessingException | NoSuchAlgorithmException | NoSuchPaddingException |
                 IllegalBlockSizeException | BadPaddingException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
}

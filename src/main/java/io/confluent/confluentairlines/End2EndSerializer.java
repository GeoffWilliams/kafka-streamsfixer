package io.confluent.confluentairlines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;


public class End2EndSerializer implements Serializer<JsonNode>  {
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
    public byte[] serialize(String topic, JsonNode data) {
        if (data == null) {
            return null;
        }
        try {
            byte[] jsonBytes = objectMapper.writeValueAsBytes(data);
            byte[] keyValue = e2eSecret.getBytes();

            Key key = new SecretKeySpec(keyValue, e2eAlgo);
            Cipher c = Cipher.getInstance(e2eAlgo);
            c.init(Cipher.ENCRYPT_MODE, key);
            return c.doFinal(jsonBytes);

        } catch (JsonProcessingException | NoSuchAlgorithmException | NoSuchPaddingException |
                 IllegalBlockSizeException | BadPaddingException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }

    }
}

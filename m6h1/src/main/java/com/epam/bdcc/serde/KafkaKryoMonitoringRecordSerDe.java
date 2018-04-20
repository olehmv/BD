package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaKryoMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {

        protected Kryo initialValue() {

            Kryo kryo = new Kryo();
            new SparkKryoHTMRegistrator().registerClasses(kryo);
            return kryo;

        };

    };

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        ByteBufferOutput output = new ByteBufferOutput(1000);
        kryos.get().writeObject(output, data);
        return output.toBytes();
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        try {

            return kryos.get().readObject(new ByteBufferInput(data), MonitoringRecord.class);

        } catch (Exception e) {

            throw new IllegalArgumentException("Error reading bytes", e);
        }


    }


    @Override
    public void close() {

    }
}

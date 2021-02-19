package com.adeo.ccr.mongodb.sink.processor;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRecordHeaderAdderTest {

  @Test
  void process() {
    Headers headers = new ConnectHeaders()
        .addDouble("DOUBLE_HEADER", 2.5d)
        .addBoolean("BOOLEAN_HEADER", true)
        .addInt("INT_HEADER", 25)
        .addString("STRING_HEADER", "header");
    SinkRecord sinkRecord = new SinkRecord(null, 0, null, null, null, null, 0, null, null, headers);
    BsonString test = new BsonString("test");
    SinkDocument sinkDocument = new SinkDocument(null, new BsonDocument(
        List.of(
            new BsonElement("currentValue", test)
        )
    ));

    KafkaRecordHeaderAdder cut = new KafkaRecordHeaderAdder(null);
    cut.process(sinkDocument, sinkRecord);
    Optional<BsonDocument> valueDoc = sinkDocument.getValueDoc();
    assertThat(valueDoc).isNotEmpty();
    BsonDocument actual = valueDoc.get();
    assertThat(actual).containsEntry("currentValue", test);
    assertThat(actual.containsKey("RECORD_HEADERS")).isTrue();
    BsonDocument recordHeaders = (BsonDocument) actual.get("RECORD_HEADERS");
    assertThat(recordHeaders).containsValues(
        new BsonString("header"),
        new BsonDouble(2.5),
        new BsonInt32(25),
        new BsonBoolean(true)
    );
  }
}
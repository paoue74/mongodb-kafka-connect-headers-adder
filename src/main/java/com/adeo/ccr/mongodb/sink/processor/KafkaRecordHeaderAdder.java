package com.adeo.ccr.mongodb.sink.processor;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class KafkaRecordHeaderAdder extends PostProcessor {

  public KafkaRecordHeaderAdder(MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(SinkDocument doc, SinkRecord orig) {
    doc.getValueDoc()
        .ifPresent(valueDoc -> valueDoc.put("RECORD_HEADERS", buildHeadersObject(orig.headers())));
  }

  private BsonDocument buildHeadersObject(Headers headers) {
    ConnectHeaders connectHeaders = Optional.of((ConnectHeaders) headers).orElseThrow();
    List<BsonElement> elements = new ArrayList<>();
    connectHeaders.forEach(header -> elements.add(buildBsonElement(header)));
    return new BsonDocument(elements);
  }

  private BsonElement buildBsonElement(Header header) {
    BsonValue element = null;
    Object value = header.value();
    if (value instanceof Integer) {
      element = new BsonInt32(((Integer) value));
    } else if (value instanceof String) {
      element = new BsonString((String) value);
    } else if (value instanceof Long) {
      element = new BsonInt64((Long) value);
    } else if (value instanceof Double) {
      element = new BsonDouble((Double) value);
    } else if(value instanceof Boolean) {
      element= new BsonBoolean((boolean) value);
    } else {
      element = new BsonNull();
    }
    return new BsonElement(header.key(), element);
  }
}

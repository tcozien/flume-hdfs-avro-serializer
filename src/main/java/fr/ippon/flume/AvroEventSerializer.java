package fr.ippon.flume;

import com.sun.tools.javac.util.Assert;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.MalformedParametersException;
import java.nio.ByteBuffer;

public class AvroEventSerializer implements EventSerializer, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(AvroEventSerializer.class);

    public static final String CONTEXT_AVRO_SCHEMA_PATH = "schema.path";
    public static final String CONTEXT_SYNC_INTERVAL_BYTES = "syncIntervalBytes";
    public static final String CONTEXT_COMPRESSION_CODEC = "compressionCodec";

    private final OutputStream out;
    private DatumWriter<Object> writer = null;
    private DataFileWriter<Object> dataFileWriter = null;
    private int syncIntervalBytes;
    private String compressionCodec;
    private String avroSchemaPath;
    private Schema schema;

    private AvroEventSerializer(OutputStream out) {
        this.out = out;
    }

    protected Schema getSchema() {
        Assert.checkNonNull(schema);
        return schema;
    }

    @Override
    public void configure(Context context) {
        avroSchemaPath = context.getString(CONTEXT_AVRO_SCHEMA_PATH, null);
        Assert.checkNonNull(avroSchemaPath);
        schema = loadFromUrl(avroSchemaPath);
        logger.info("Avro schema loaded = " + schema.toString());

        syncIntervalBytes = context.getInteger(CONTEXT_SYNC_INTERVAL_BYTES, Integer.valueOf(2048000)).intValue();
        compressionCodec = context.getString(CONTEXT_COMPRESSION_CODEC, "null");
        initializeWriter();
    }

    private Schema loadFromUrl(String schemaUrl) {
        Configuration conf = new Configuration();
        Schema.Parser parser = new Schema.Parser();
        FSDataInputStream input = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            input = fs.open(new Path("hdfs://" + schemaUrl));
            return parser.parse(input);
        } catch (Exception e) {
            throw new MalformedParametersException("Error while loading hdfs avro schema");
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    logger.error("Error while closing input Avro schema.", e);
                }
            }
        }
    }

    private void initializeWriter() {
        writer = new GenericDatumWriter<Object>(this.getSchema());
        dataFileWriter = new DataFileWriter<Object>(writer);
        dataFileWriter.setSyncInterval(syncIntervalBytes);
        try {
            CodecFactory e = CodecFactory.fromString(compressionCodec);
            this.dataFileWriter.setCodec(e);
        } catch (AvroRuntimeException var5) {
            logger.warn("Unable to instantiate avro codec with name (" + compressionCodec + "). Compression disabled. Exception follows.", var5);
        }
    }

    @Override
    public void afterCreate() throws IOException {
        this.dataFileWriter.create(this.getSchema(), this.out);
    }

    @Override
    public void afterReopen() throws IOException {
        throw new UnsupportedOperationException("Avro API doesn't support append");
    }

    @Override
    public void write(Event event) throws IOException {
        dataFileWriter.appendEncoded(ByteBuffer.wrap(event.getBody()));
    }

    @Override
    public void flush() throws IOException {
        dataFileWriter.flush();
    }

    @Override
    public void beforeClose() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public boolean supportsReopen() {
        return false;
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            AvroEventSerializer writer = new AvroEventSerializer(out);
            writer.configure(context);
            return writer;
        }
    }
}
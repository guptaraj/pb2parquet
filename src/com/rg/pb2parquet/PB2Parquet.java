package com.rg.pb2parquet;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;

public class PB2Parquet {

    public static void main(String[] args)
    {
        System.out.println("TODO: read input message and convert to parquet");
    }

    public static void writeMessages(Class<? extends Message> cls, Path file,
                                     MessageOrBuilder... records) throws IOException {
        ProtoParquetWriter<MessageOrBuilder>
        writer = new ProtoParquetWriter<>(file, cls);

        try {
            for (MessageOrBuilder record : records) {
                writer.write(record);
            }
        } finally {
            writer.close();
        }
    }
}
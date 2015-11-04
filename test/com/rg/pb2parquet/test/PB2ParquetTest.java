package com.rg.pb2parquet.test;

/**
 * Created by rgupta on 11/2/15.
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.rg.pb2parquet.PB2Parquet;
import com.twitter.data.proto.tutorial.AddressBookProtos;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class PB2ParquetTest {

    @Test
    public void pb2parquetTest(){
     //assert(true);
        //Create addressbook
        AddressBookProtos.Person person = AddressBookProtos.Person.newBuilder()
                .setEmail("test@abc.com")
                .setId(1)
                .setName("abc")
                .addPhone(
                        AddressBookProtos.Person.PhoneNumber.newBuilder()
                                .setNumber("201000000")
                                .setType(AddressBookProtos.Person.PhoneType.HOME)
                                .build()
                ).build();

        Path p = new Path("test-artifacts/person.parquet");
        try {
            PB2Parquet.writeMessages(AddressBookProtos.Person.class, p, person);
        } catch (IOException e) {
          fail("Unable to write parquet file. " + e.getMessage());
        }

    }
}

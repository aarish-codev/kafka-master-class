package avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordDemo {

    public static void main(String[] args) {

        // 0. Define avro schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\": \"Avro Schema for our Customer\",     \n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");
        // 1.Create a generic record
        GenericRecordBuilder firstRecord = new GenericRecordBuilder(schema);
        firstRecord.set("first_name", "John");
        firstRecord.set("last_name", "Doe");
        firstRecord.set("age", 26);
        firstRecord.set("height", 175f);
        firstRecord.set("weight", 70.5f);
        firstRecord.set("automated_email", false);
        GenericData.Record firstCustomer = firstRecord.build();
        System.out.println(firstCustomer);

        GenericRecordBuilder secondCustomer = new GenericRecordBuilder(schema);
        secondCustomer.set("first_name", "John");
        secondCustomer.set("last_name", "Doe");
        secondCustomer.set("age", 26);
        secondCustomer.set("height", 175f);
        secondCustomer.set("weight", 70.5f);
        GenericData.Record customerWithDefaultEmail = secondCustomer.build();
        System.out.println(customerWithDefaultEmail);

        GenericRecordBuilder invalidCustomer = new GenericRecordBuilder(schema);
        invalidCustomer.set("height", "blahblah");
        invalidCustomer.set("first_name", "John");
        invalidCustomer.set("last_name", "Doe");
        invalidCustomer.set("age", 26);
        invalidCustomer.set("weight", 26f);
        invalidCustomer.set("automated_email", 70);
        try {
            GenericData.Record wrong = invalidCustomer.build();
            System.out.println(wrong);
        } catch (AvroRuntimeException e) {
            System.out.println("Generic Record build did not succeed");
            e.printStackTrace();
        }

        // 2.Write generic record to file
        String pathname = "kafka-avro/src/main/resources/avro/customer-generic.avro";
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(schema, new File(pathname));
            dataFileWriter.append(firstCustomer);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // 3.Read generic record from file
        final File file = new File(pathname);
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));

            // read a non existent field
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        } catch (AvroRuntimeException | IOException e) {
            System.out.println("Error while reading avro schema: " + e.getMessage());

        }
        // 4.interpret as avro generic record
    }
}
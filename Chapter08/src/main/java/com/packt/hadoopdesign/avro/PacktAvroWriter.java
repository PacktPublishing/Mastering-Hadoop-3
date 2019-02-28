package com.packt.hadoopdesign.avro;
import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import packt.Author;

public class PacktAvroWriter {
    public static void main(String args[]) throws IOException{

        Author author1=new Author();

        author1.setAuthorName("chanchal singh");
        author1.setAuthorId("PACKT-001");
        author1.setAuthorAddress("Pune India");

        Author author2=new Author();

        author2.setAuthorName("Manish Kumar");
        author2.setAuthorId("PACKT-002");
        author2.setAuthorAddress("Mumbai India");

        Author author3=new Author();

        author3.setAuthorName("Dr.Tim");
        author3.setAuthorId("PACKT-003");
        author3.setAuthorAddress("Toronto Canada");


        DatumWriter<Author> empDatumWriter = new SpecificDatumWriter<Author>(Author.class);
        DataFileWriter<Author> empFileWriter = new DataFileWriter<Author>(empDatumWriter);

        empFileWriter.create(author1.getSchema(), new File("/home/exa00077/avro/author.avro"));

        empFileWriter.append(author1);
        empFileWriter.append(author2);
        empFileWriter.append(author3);

        empFileWriter.close();

        System.out.println("Succesfully Created Avro file");
    }
}

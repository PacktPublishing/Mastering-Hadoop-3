package com.packt.hadoopdesign.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import packt.Author;

import java.io.File;
import java.io.IOException;

public class PacktAvroReader {
    public static void main(String args[]) throws IOException{

        DatumReader<Author> authorDatumReader = new SpecificDatumReader<Author>(Author.class);

        DataFileReader<Author> authorFileReader = new DataFileReader<Author>(new
                File("/home/packt/avro/author.avro"), authorDatumReader);
        Author author=null;

        while(authorFileReader.hasNext()){

            author=authorFileReader.next(author);
            System.out.println(author);
        }
    }
}

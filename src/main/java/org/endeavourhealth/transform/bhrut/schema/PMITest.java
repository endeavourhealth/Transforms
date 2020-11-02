package org.endeavourhealth.transform.bhrut.schema;

//import static org.junit.jupiter.api.Assertions.*;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;

//import com.univocity.parsers.common.record.Record;
//import com.univocity.parsers.conversions.Conversions;
//import com.univocity.parsers.csv.CsvParser;
//import com.univocity.parsers.csv.CsvParserSettings;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.hibernate.criterion.Example;
import org.hl7.fhir.instance.model.Organization;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;
//import

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;
//import com.univocity.parsers.csv.CsvParser;
//import com.univocity.parsers.csv.CsvParserSettings;
//import ca.uhn.fhir.context.FhirContext;


class PMITest {
    // Simple classes to test Bhrut methods etc.
    //
    private static final String SAMPLE_CSV_FILE_PATH1 = "D:/Endeavour/test/pcr/test.csv";
    private static final String SAMPLE_CSV_FILE_PATH2 = "./test.csv";

//    public static  void main(String[] args) throws Exception {
//        Gson g = new Gson();


//            OdsOrganisation or = lookupOrganisationViaRest("RF4");
//            System.out.println(or.getOrganisationName());
//            OdsOrganisation or2 = lookupOrganisationViaRest("RF4TC");
//            System.out.println(or2.getOrganisationName());
//        }
//
//
//    }

//    public static void main(String[] args) throws IOException {
//        // Reader reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH));
//        CsvParserSettings settings = new CsvParserSettings();
//        //settings.getFormat().setLineSeparator("\n");
//        settings.setHeaderExtractionEnabled(true);
//        settings.getFormat().setDelimiter(',');
//        settings.setParseUnescapedQuotes(true);
//        CsvParser csvParser = new CsvParser(settings);
//        //Reader reader = Files.newBufferedReader(Paths.get(new File(SAMPLE_CSV_FILE_PATH));
//        //csvParser.beginParsing(reader);
//        InputStream resourceAsStream = PMITest.class.getClassLoader().getResourceAsStream(SAMPLE_CSV_FILE_PATH);
//
//        csvParser.beginParsing(resourceAsStream);
//        csvParser.getRecordMetadata().convertFields(Conversions.replace("\"\"", "\""));
//
//        //csvParser.beginParsing(getReader(SAMPLE_CSV_FILE_PATH));
//        Record record;
//        while ((record = csvParser.parseNextRecord()) != null) {
//            System.out.println(">" + record);
//            System.out.println(record.getString("h2"));
//        }
//
////       List<Record> resolvedData = csvParser.parseAllRecords(new FileReader(SAMPLE_CSV_FILE_PATH));
////        // 3rd, process the matrix with business logic
////        for (Record rec : resolvedData) {
////                    System.out.println(rec);
////                    System.out.println("col1=" + rec.getString(0));
////                    System.out.println("col2=" + rec.getString(1));
////                    System.out.println("col3=" + rec.getString(2));
////                    System.out.println("col4=" + rec.getString(3));
////        }
//        // String[] values = new CsvParser(settings).parseLine("example1;\"example with \" inside\";example3");
//        //for(String value : values){
//        //  System.out.println(value);
//        //}
//    }


//        public static void main(String[] args) throws IOException {
//        try (
//                Reader reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH1));
////                CSVParser csvParser = new CSVParser(reader,CSVFormat.RFC4180.withFirstRecordAsHeader()
////                    .withQuote(' ')
////                        .withQuote('"'))
// CSVParser csvParser = new CSVParser(reader,CSVFormat.RFC4180.withFirstRecordAsHeader().withQuote('"')
//                    .withQuote(' ')
//                    .withEscape('\'').withHeader("h1","h2","h3","h4")
// .withSkipHeaderRecord());
//                 //   .withEscape('\''))
//                        //.withEscape('\\'));.withQuote('"')
//                        //.withQuoteMode(QuoteMode.MINIMAL));
//                        //.withEscape('\\').withQuoteMode(QuoteMode.NONE));
//              //   BhrutCsvToFhirTransformer.CSV_FORMAT);
//                //CSVFormat.RFC4180);
//
//        ) {
//
//
//            for (CSVRecord csvRecord : csvParser) {
//                // Accessing Values by Column Index
////                String c1 = csvRecord.get(0).replace("\""," ").trim();
////                String c2 = csvRecord.get(1).replace("\""," ").trim();;
////                String c3 = csvRecord.get(2).replace("\""," ").trim();;
////                String c4 = csvRecord.get(3).replace("\""," ").trim();;
////                String c5 = csvRecord.get(4).replace("\""," ").trim();;
//                System.out.println(csvRecord.get("h1"));
//                String c1 = csvRecord.get("h1");
//                String c2 = csvRecord.get("h2");
//                String c3 = csvRecord.get("h3");
//                String c4 = csvRecord.get("h4");
//                System.out.println("Record No - " + csvRecord.getRecordNumber());
//                System.out.println("---------------");
//                System.out.println("C1 : " + c1);
//                System.out.println("C2 : " + c2);
//                System.out.println("C3 : " + c3);
//                System.out.println("C4 : " + c4);
//                System.out.println("---------------\n\n");
//            }
//        }
//    }
//    public static Reader getReader(String relativePath) {
//        try {
//            return new InputStreamReader(Example.class.getResourceAsStream(relativePath), "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            throw new IllegalStateException("Unable to read input", e);
//        }
//    }

    public static void main(String[] args) throws Exception {
        BhrutCsvHelper csvHelper = new BhrutCsvHelper(null,null,null);
//        String code = "RF4MW";
//        if (csvHelper.isRF4Child(code)) {
//            System.out.println("Yes to " + code);
//        } else {
//            System.out.println("No to " + code);
//        }
//        code = "error";
//        if (csvHelper.isRF4Child(code)) {
//            System.out.println("Yes to " + code);
//        } else {
//            System.out.println("No to " + code);
//        }
//
//                code = "V81997";
//        if (csvHelper.isRF4Child(code)) {
//            System.out.println("Yes to " + code);
//        } else {
//            System.out.println("No to " + code);
//        }
//        code = "RF4MW";
//        if (csvHelper.isRF4Child(code)) {
//            System.out.println("Yes to " + code);
//        } else {
//            System.out.println("No to " + code);
//        }
    }


}

package org.endeavourhealth.transform.common;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * confusingly, this class deals with APPLICATION resource files - not FHIR resources
 */
public class ResourceParser {

    public static Map<String, String> readCsvResourceIntoMap(String resourcePath, String keyColumn, String valueColumn, CSVFormat format) throws Exception {

        ClassLoader classLoader = new ResourceParser().getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        InputStreamReader isr = new InputStreamReader(inputStream);
        try {
            CSVParser parser = new CSVParser(isr, format);

            Map<String, Integer> headers = parser.getHeaderMap();
            if (!headers.containsKey(keyColumn)) {
                throw new Exception("No column [" + keyColumn + "] found in headers: " + headers.keySet());
            }
            if (!headers.containsKey(valueColumn)) {
                throw new Exception("No column [" + valueColumn + "] found in headers: " + headers.keySet());
            }

            Map<String, String> ret = new HashMap<>();

            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                String key = record.get(keyColumn);
                String value = record.get(valueColumn);
                ret.put(key, value);
            }

            return ret;

        } finally {
            isr.close();
            inputStream.close();
        }
    }
}

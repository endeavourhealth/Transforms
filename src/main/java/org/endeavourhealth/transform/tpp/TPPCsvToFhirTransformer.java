package org.endeavourhealth.transform.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.*;
import org.endeavourhealth.transform.barts.transforms.*;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class TPPCsvToFhirTransformer {
//TODO - totally stripped version just for final static strings for compilation
    private static final Logger LOG = LoggerFactory.getLogger(TPPCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
            .withHeader()
            .withDelimiter(',')
            .withEscape((Character) null)
            .withQuote('"')
            .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this

    public static final CSVFormat CSV_FORMAT_NO_HEADER = CSVFormat.DEFAULT
            .withDelimiter(',')
            .withEscape((Character) null)
            .withQuote((Character) null)
            .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this

    //TODO - check these
    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";




}

package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CLINICALCODES;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CLINICALCODESPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CLINICALCODESPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CLINICALCODES.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CLINICALCODES) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(CLINICALCODES parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();
        CsvCell readCode = parser.getCode();

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        //create a unique observation Id
        String observationId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + readCode.getString();

        //cache the link between the observation and consultation
        if (!consultationId.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(consultationId,
                    patientId.getString(),
                    observationId,
                    ResourceType.Observation);
        }
    }
}

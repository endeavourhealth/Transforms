package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PRESCRIPTIONS;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PRESCRIPTIONSPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PRESCRIPTIONSPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PRESCRIPTIONS.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((PRESCRIPTIONS) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }

    public static void createResource(PRESCRIPTIONS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();
        CsvCell drugName = parser.getDrugName();
        CsvCell quantity = parser.getQuanity();

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        String drugNameFirstPart = drugName.getString().substring(0, drugName.getString().indexOf(" "));
        drugNameFirstPart = drugNameFirstPart.replace("/","").trim();   //strip out slashes as used as a reference
        String drugQty = quantity.getString();

        //create a unique Id for the drug based on case : consultation : drugName + qty
        String drugId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + drugNameFirstPart.concat(drugQty);

        //linked consultation encounter record
        if (!consultationId.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(consultationId,
                    drugId,
                    ResourceType.MedicationStatement);
        }
    }
}

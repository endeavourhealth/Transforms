package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.FamilyHistory;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.FamilyMemberHistoryBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FamilyHistoryTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(FamilyHistoryTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createResource((FamilyHistory) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void createResource(FamilyHistory parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        CsvCell milleniumPersonIdCell = parser.getPersonId();
        if (milleniumPersonIdCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "Skipping FamilyHistory record for {} as no patient record found", milleniumPersonIdCell);
            return;
        }
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        if (patientBuilder == null) {
            TransformWarnings.log(LOG, parser, "Skipping FamilyHistory record for {} as no patient record found", milleniumPersonIdCell);
            return;
        }

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();
        Patient patient = (Patient) patientBuilder.getResource();
        Reference patientReference = new Reference(patient);
        familyMemberHistoryBuilder.setPatient(patientReference, milleniumPersonIdCell);

        CsvCell dateTime = parser.getCreateDtTm();
        if (!dateTime.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(dateTime.getDateTime());
            familyMemberHistoryBuilder.setDate(dateTimeType, dateTime);
        }

       //TODO - not convinced it's complete or how we determine status so defaulting to Partial
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.PARTIAL);

        CsvCell relatedPersonCell = parser.getRelatedPersonId();
        if (!relatedPersonCell.isEmpty()) {
            // Retrieve related person from cache or db
            // Need name, relationship, gender
        }



        //TODO - parser implementation
    }
}

package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPAGP;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPAGPTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPAGPTransformer.class);

    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createPatientGP((PPAGP)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }


    public static void createPatientGP(PPAGP parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        if (patientBuilder == null) {
            LOG.warn("Skipping PPAGP record for " + milleniumPersonIdCell.getString() + " as no MRN->Person mapping found");
            return;
        }

        //if we don't have a person ID, there's nothing we can do with the row
        CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
        if (personnelId.isEmpty()) {
            return;
        }
        ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelId);
        Reference practitionerReference = csvHelper.createPractitionerReference(practitionerResourceId.getResourceId().toString());

        //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell endDateCell = parser.getEndEffectiveDate();
        if (!activeCell.getIntAsBoolean()
                && !endDateCell.isEmpty()) {

            //this only removes if the reference matches the record we're supposed to remove, in case we've already
            //processed another row telling us to change it
            patientBuilder.removeCareProvider(practitionerReference);

        } else {
            patientBuilder.addCareProvider(practitionerReference);
        }
    }

    /*public static void createPatientGP(PPAGP parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {



        Patient fhirPatient = PatientResourceCache.getPatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()));

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        if (fhirPatient == null) {
            if (parser.isActive()) {
                LOG.warn("Patient Resource Not Found In Cache: " + parser.getMillenniumPersonIdentifier());
            } else {
                return;
            }
        }

        fhirPatient.addCareProvider(csvHelper.createPractitionerReference(parser.getRegisteredGPMillenniumPersonnelId()));

        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

    }*/
}

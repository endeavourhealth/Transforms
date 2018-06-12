package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPAGP;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class PPAGPTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPAGPTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createPatientGP((PPAGP) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
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
            TransformWarnings.log(LOG, parser, "Skipping PPAGP record for {} as no MRN->Person mapping found", milleniumPersonIdCell);
            return;
        }

        //the relation code links to the standard code ref table, and defines the type of relationship
        //we're only interested in who the patients registered GP is
        CsvCell relationshipType = parser.getPersonPersonnelRelationCode();
        if (!relationshipType.isEmpty()) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICIAL_RELATIONSHIP_TYPE, relationshipType);
            if (codeRef != null) {
                String display = codeRef.getCodeDispTxt();
                if (!display.equalsIgnoreCase("Registered GP")) {
                    return;
                    //throw new TransformException("PPAGP record has unexpected relation code " + relationshipType.getLong());
                }
            }
        }

        //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell endDateCell = parser.getEndEffectiveDate();
        boolean delete = !activeCell.getIntAsBoolean()
                || !BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell); //note that the Cerner end of time is used for active record end dates

        CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelId)) {
            ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelId);
            Reference practitionerReference = csvHelper.createPractitionerReference(practitionerResourceId.getResourceId().toString());

            if (delete) {
                patientBuilder.removeCareProvider(practitionerReference);

            } else {
                patientBuilder.addCareProvider(practitionerReference);
            }
        }

        CsvCell orgId = parser.getRegisteredGPPracticeMillenniumIdOrganisationCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(orgId)) {

            //the ORGREF file is mapped using the standard ID mapper, so we need to convert the ID to UUID using this approach
            UUID globallyUniqueId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Organization, orgId.getString());
            Reference orgReference = ReferenceHelper.createReference(ResourceType.Organization, globallyUniqueId.toString());

            if (delete) {
                patientBuilder.removeCareProvider(orgReference);

            } else {
                patientBuilder.addCareProvider(orgReference);
            }
        }
    }

}

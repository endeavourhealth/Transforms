package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPAGP;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class PPAGPTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPAGPTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientGP((PPAGP)parser, fhirResourceFiler, csvHelper, sourceSystem);
            }
        }
    }

    public static void createPatientGP(PPAGP parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        CsvCell relationshipTypeCell = parser.getPersonPersonnelRelationCode();

        if (NwlExtractsCsvHelper.isEmptyOrIsZero(relationshipTypeCell)) {
            return;
        }

        //the relation code links to the standard code ref table, and defines the type of relationship
        //we're only interested in who the patients registered GP is
        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICIAL_RELATIONSHIP_TYPE, relationshipTypeCell);
       if(null!=codeRef) {
           String display = codeRef.getCodeDispTxt();
           if (!display.equalsIgnoreCase("Registered GP")) {
               return;
               //throw new TransformException("PPAGP record has unexpected relation code " + relationshipType.getLong());
           }
       }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //we get no person ID when it's a delete, but we also get a replacement record for the person, adding the n
        //new GP details, which will replace what's on the resource. So just return out here.
        if (personIdCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "Patient Person ID for the GP ID in PPAGP {} not found", relationshipTypeCell);
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
        if (patientBuilder == null) {
            return;
        }

        Reference practitionerReference=null;

        try {
            //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
            CsvCell activeCell = parser.getActiveIndicator();
            CsvCell endDateCell = parser.getEndEffectiveDate();

            //NwlExtracts send multiple records per patient, with the end date and active indicator telling us which are active or not
            boolean isActive = activeCell.getIntAsBoolean()
                    && NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(endDateCell);

            //if this record isn't active, ignore it. A separate record should also be present to give us the
            //current details, which will be processed.
            if (!isActive) {
                return;
            }

            //remove any existing care providers, since we only support having one current org and one current practitioner
            patientBuilder.removeAllCareProviders();

            //add the GP, if present
            CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
            if (NwlExtractsCsvHelper.isEmptyOrIsZero(personnelId)) {
                TransformWarnings.log(LOG, parser, "PersonnelId Code is either zero or empty for the Patient ID {} in PPAGP", personIdCell);
               // SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "personnelId Code not found for the Patient ID"+ personIdCell + "for ODS Code" + NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);

            } else {
                practitionerReference = csvHelper.createPractitionerReference(personnelId);
                if (patientBuilder.isIdMapped()) {
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
                }

                patientBuilder.addCareProvider(practitionerReference, personnelId);

                Reference practitionerReferenceUuid = null;
                ReferenceComponents components = ReferenceHelper.getReferenceComponents(practitionerReference);
                try{
                    UUID uuid = UUID.fromString(components.getId());
                    practitionerReferenceUuid = practitionerReference;
                }
                catch (IllegalArgumentException exception){
                    practitionerReferenceUuid = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
                }

                ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(patientBuilder);
                containedParametersBuilder.removeContainedParameters();
                containedParametersBuilder.addParameter(TransformConstant.PARAMETER_GP_PRACTITIONER_ID,
                        practitionerReferenceUuid);
            }

            //add the GP practice
            CsvCell orgIdCell = parser.getRegisteredGPPracticeMillenniumIdOrganisationCode();
            Reference orgReference = null;
            if (NwlExtractsCsvHelper.isEmptyOrIsZero(orgIdCell)) {
                TransformWarnings.log(LOG, parser, "OrgIdCell Code is either zero or empty for the Patient ID {} in PPAGP ", personIdCell);
              //  SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "orgIdCell Code not found for the Patient ID"+ personIdCell + "for ODS Code" + NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);

            } else {

                orgReference = csvHelper.createOrganizationReference(orgIdCell);
                if (patientBuilder.isIdMapped()) {
                    orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, csvHelper);
                }

                patientBuilder.addCareProvider(orgReference, orgIdCell);
            }

            Practitioner existingPractitioner = (Practitioner)csvHelper.retrieveResourceForLocalId(ResourceType.Practitioner, personnelId);

            if(existingPractitioner!=null && !NwlExtractsCsvHelper.isEmptyOrIsZero(orgIdCell)) {
                PractitionerBuilder practitionerBuilder = new PractitionerBuilder(existingPractitioner);
                Reference orgReferenceForPractitioner = csvHelper.createOrganizationReference(orgIdCell);
                if (practitionerBuilder.isIdMapped()) {
                    orgReferenceForPractitioner = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferenceForPractitioner, csvHelper);
                }
                if(personnelId.getString()!=null && orgIdCell.getString()!=null && orgReferenceForPractitioner != null){

                  if(null!= practitionerBuilder.getRole()){
                      practitionerBuilder.getRole().setManagingOrganization(orgReferenceForPractitioner);
                  } else {
                      practitionerBuilder.addRole().setManagingOrganization(orgReferenceForPractitioner);
                  }
                    fhirResourceFiler.saveAdminResource(null, false, practitionerBuilder);
                }
            }

        } finally {
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        }
    }
}

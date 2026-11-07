package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.datasharingmanager.models.OrganisationEntity;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.helpers.NwlExtractsHelper;
import org.endeavourhealth.transform.nwlextracts.schema.ENCNT;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;


public class EpisodeOfCareTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        NwlExtractsHelper nwlextractsHelper = new NwlExtractsHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null);
        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                transformEpisodeOfCare((ENCNT) parser, fhirResourceFiler, csvHelper,nwlextractsHelper,sourceSystem);
            }
        }
    }

    private static void transformEpisodeOfCare(ENCNT parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, NwlExtractsHelper nwlextractsHelper, String sourceSystem) throws Exception {

        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare ;
        CsvCell visitIdCell = parser.getVisitId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        String personId=personIdCell.getString();
        String visitId=null;
        if (!visitIdCell.isEmpty()){
            visitId=visitIdCell.getString();
        }

        if(visitId != null) {
            fhirEpisodeOfCare = (EpisodeOfCare) nwlextractsHelper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else {
                episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            }

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCareBuilder);
            identifierBuilder.setSystem("");
            identifierBuilder.setValue(visitId);

            episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);

            String encounterTypeCls=null;

            CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
            Encounter.EncounterClass cls = ENCNTTransformer.getEncounterClass(encounterTypeCodeCell, csvHelper);
            if (cls != null) {
                encounterTypeCls = cls.getClass().toString();
            }

            CsvCell recordedDateCell = parser.getEncounterCreateDateTime();
            CsvCell enctEffectiveDateCell = parser.getEncounterEffectiveDateTime();

            Date recordedDt=null;
            Date effectiveDt=null;


            if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(recordedDateCell)) {
                recordedDt = NwlExtractsCsvHelper.parseDate(recordedDateCell);
            }

            if (null!= enctEffectiveDateCell) {
                effectiveDt = NwlExtractsCsvHelper.parseDate(enctEffectiveDateCell);
            }

            if (null != encounterTypeCls) {
                if (encounterTypeCls.equalsIgnoreCase("INPATIENT") || encounterTypeCls.equalsIgnoreCase("OUTPATIENT") || encounterTypeCls.equalsIgnoreCase("EMERGENCY")) {
                    episodeOfCareBuilder.setRegistrationStartDate(effectiveDt);
                } else {
                    episodeOfCareBuilder.setRegistrationStartDate(recordedDt);
                }
            }

            OrganizationBuilder organizationBuilder = createOrgBuilder(sourceSystem);
            CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
            String practitionerId=null;

            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(responsibleHCPCell)) {

              practitionerId=responsibleHCPCell.getString();
            }

            if (newEpisodeOfCare) {
                episodeOfCareBuilder.setPatient(NwlExtractsHelper.createReference(ResourceType.Patient, personId));
                episodeOfCareBuilder.setManagingOrganisation(NwlExtractsHelper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
                if (practitionerId!=null) {
                    episodeOfCareBuilder.setCareManager(NwlExtractsHelper.createReference(ResourceType.Practitioner, practitionerId));
                }
            }
            else {
                Reference patientReference = NwlExtractsHelper.createPatientReference(personId);
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                episodeOfCareBuilder.setPatient(patientReference);
                Reference organizationReference = NwlExtractsHelper.createOrganizationReference(organizationBuilder.getResourceId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                episodeOfCareBuilder.setManagingOrganisation(organizationReference);
                if (practitionerId!=null) {
                    Reference practitionerReference = NwlExtractsHelper.createPractitionerReference(practitionerId);
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                    episodeOfCareBuilder.setCareManager(practitionerReference);
                }
            }

            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
        }

    }
    private static OrganizationBuilder createOrgBuilder(String odsCode) throws Exception {

        OrganisationEntity org = OrganisationCache.getOrganisationDetailsFromOdsCode(odsCode);
        String name = org.getName();

        OrganizationBuilder  organization = new OrganizationBuilder();
        organization.setId(name);
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organization);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(odsCode);

        organization.setName(name);
        return organization;
    }
}

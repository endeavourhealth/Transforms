package org.endeavourhealth.transform.nwlextracts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.ENCNT;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EncounterEventTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterEventTransformer.class);

    public static void createEncounterEvents(ENCNT parser, EncounterBuilder parentEncounterBuilder,
                                             FhirResourceFiler fhirResourceFiler,
                                             NwlExtractsCsvHelper csvHelper) throws Exception {



        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell visitIdCell = parser.getVisitId();
        CsvCell mainSpecialtyCodeCell = parser.getMainSpecialtyMillenniumCode();
        CsvCell enctEffectiveDateCell = parser.getEncounterCreateDateTime();
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        String typeDesc=null;

        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
            if (codeRef != null) {
                typeDesc = codeRef.getCodeDispTxt();
            }
        }
                    String encounterId= visitIdCell.getString() + ":" + encounterIdCell.getString();

                    ContainedListBuilder existingEncounterList = new ContainedListBuilder(parentEncounterBuilder);
                    EncounterBuilder childEncounterBuilder = new EncounterBuilder();

                    Encounter.EncounterClass cls = ENCNTTransformer.getEncounterClass(encounterTypeCodeCell, csvHelper);

                    if (cls == null) {
                        return;
                    } else {
                        childEncounterBuilder.setClass(cls, encounterTypeCodeCell);
                    }

                    childEncounterBuilder.setType(typeDesc, encounterTypeCodeCell);

                    childEncounterBuilder.setId(encounterId);

                     if (!NwlExtractsCsvHelper.isEmptyOrIsZero(personIdCell)){
                        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
                        if (childEncounterBuilder.isIdMapped()) {
                            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                        }
                        childEncounterBuilder.setPatient(patientReference);
                    }

                    if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(enctEffectiveDateCell)) {
                        Date  effectiveDt = NwlExtractsCsvHelper.parseDate(enctEffectiveDateCell);
                        childEncounterBuilder.setPeriodStart(effectiveDt, enctEffectiveDateCell);
                    }

                    if (!NwlExtractsCsvHelper.isEmptyOrIsZero(visitIdCell)) {
                        Reference episodeReference
                                = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, visitIdCell.getString());
                        if (childEncounterBuilder.isIdMapped()) {
                            episodeReference
                                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, fhirResourceFiler);
                        }
                        childEncounterBuilder.setEpisodeOfCare(episodeReference);
                    }

                    // Location
                    CsvCell locationRefCell = parser.getCurrentLocationIdentifier();

                    if (NwlExtractsCsvHelper.isEmptyOrIsZero(locationRefCell)) {
                        TransformWarnings.log(LOG, parser, "Encounter Location  is not found in ENCNT for the Encounter ID {} and Patient ID {}", encounterIdCell, personIdCell);

                    } else {

                        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, locationRefCell);

                        if (codeRef != null) {
                            String codeDesc = codeRef.getCodeDescTxt();
                            String codeDisp = codeRef.getCodeDispTxt();

                            if (Strings.isNullOrEmpty(codeDesc) && Strings.isNullOrEmpty(codeDisp)) {
                                TransformWarnings.log(LOG, parser, "Encounter Location Name is NULL in ENCNT for the Encounter ID {} and Location ID {} ", encounterIdCell, locationRefCell);
                            }
                        }

                        Reference patientAssignedLocReference
                                = ReferenceHelper.createReference(ResourceType.Location, locationRefCell.getString());
                        if (childEncounterBuilder.isIdMapped()) {
                            patientAssignedLocReference
                                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientAssignedLocReference, fhirResourceFiler);
                        }

                        if (childEncounterBuilder.getLocation().size() > 0) {
                            for (int i = 0; i < childEncounterBuilder.getLocation().size(); i++) {
                                childEncounterBuilder.getLocation().remove(i);
                            }
                        }

                        childEncounterBuilder.addLocation(patientAssignedLocReference);
                        childEncounterBuilder.getLocation().get(childEncounterBuilder.getLocation().size() - 1).setStatus(Encounter.EncounterLocationStatus.ACTIVE);
                    }

                    // responsible person
                    CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
                    if (NwlExtractsCsvHelper.isEmptyOrIsZero(responsibleHCPCell)) {
                        TransformWarnings.log(LOG, parser, "ResponsibleHCPCell Code is not found in ENCNT for the encounter ID {} and Patient ID {} ", encounterIdCell, personIdCell);

                    } else {

                        Reference practitionerReference = csvHelper.createPractitionerReference(responsibleHCPCell);
                        if (childEncounterBuilder.isIdMapped()) {
                            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                        }

                        childEncounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, true, responsibleHCPCell);
                    }

                    if (!NwlExtractsCsvHelper.isEmptyOrIsZero(mainSpecialtyCodeCell)) {
                        Reference organisationReference = csvHelper.createSpecialtyOrganisationReference(mainSpecialtyCodeCell);
                        if (childEncounterBuilder.isIdMapped()) {
                            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
                        }
                        childEncounterBuilder.setServiceProvider(organisationReference);
                    }

                    if (!NwlExtractsCsvHelper.isEmptyOrIsZero(visitIdCell)) {
                        Reference parentEncounter
                                = ReferenceHelper.createReference(ResourceType.Encounter, visitIdCell.getString());

                        parentEncounter
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, fhirResourceFiler);

                        childEncounterBuilder.setPartOf(parentEncounter);
                    }

                    //and link the parent to this new child encounter
                    Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
                    if (parentEncounterBuilder.isIdMapped()) {
                        childDischargeRef
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, fhirResourceFiler);
                    }
                    existingEncounterList.addReference(childDischargeRef);
                    fhirResourceFiler.savePatientResource(null, childEncounterBuilder);
    }
}
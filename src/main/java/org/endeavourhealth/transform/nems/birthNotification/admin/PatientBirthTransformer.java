package org.endeavourhealth.transform.nems.birthNotification.admin;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.nems.NemsXmlHelper;
import org.endeavourhealth.transform.nems.schema.v1.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBElement;
import java.io.Serializable;
import java.util.*;

import static org.endeavourhealth.transform.nems.NemsXmlHelper.*;
import static org.endeavourhealth.transform.nems.DataCache.*;

public class PatientBirthTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientBirthTransformer.class);


    public static void transform(BundleType bundle, FhirResourceFiler processor,  NemsXmlHelper xmlHelper) throws Exception {

        if(NemsXmlHelper.getValidBatch()) {
            List<EntryType> entriesForRemoval = new ArrayList<>();

            removeEntries(bundle, entriesForRemoval);
            for (EntryType entry : bundle.getEntry()) {
                PatientType patient = entry.getResource().getPatient();
                if (null != patient) {
                    LOG.info("Beginning patient transform for patient: " + patient.getId().getValue());
                    processPatient(patient, xmlHelper);
                    entriesForRemoval.add(entry);
                }
            }

            removeEntries(bundle, entriesForRemoval);
            for (EntryType entry : bundle.getEntry()) {
                RelatedPersonType relatedPerson = entry.getResource().getRelatedPerson();
                if (null != relatedPerson) {
                    processRelatedPerson(relatedPerson, xmlHelper);
                    entriesForRemoval.add(entry);
                }
            }
            removeEntries(bundle, entriesForRemoval);

            for (EntryType entry : bundle.getEntry()) {
                EpisodeOfCareType eoc = entry.getResource().getEpisodeOfCare();
                if (null != eoc) {
                    List<Serializable> content = eoc.getType().getContent();
                    for (int i = 0; i < content.size(); i++) {
                        if (content.get(i) instanceof JAXBElement) {
                            if (((JAXBElement<?>) content.get(i)).getValue() instanceof CodingType) {
                                CodingType value = ((JAXBElement<CodingType>) content.get(i)).getValue();

                                if (value.getCode().getValue().equals("delivery-place")) {
                                    processEpisodeOfCare(eoc, processor, xmlHelper);
                                }
                            }
                        }
                    }
                    entriesForRemoval.add(entry);
                }
            }
            removeEntries(bundle, entriesForRemoval);

            savePatients(processor);

            for (EntryType entry : bundle.getEntry()) {
                ObservationType observation = entry.getResource().getObservation();
                if (null != observation) {
                    processObservation(observation, xmlHelper);
                }
            }
            saveObservations(processor);
            saveEpisodesOfCare(processor);
            saveEncounters(processor);

            clearCache();
        }
    }

    private static void removeEntries(BundleType bundle, List<EntryType> entriesForRemoval) {
        for (EntryType removal : entriesForRemoval) {
            bundle.getEntry().remove(removal);
        }
        entriesForRemoval = new ArrayList<>();
    }

    //TESTING

    static void processObservation(ObservationType observation, NemsXmlHelper xmlHelper) throws Exception {
        ObservationBuilder observationBuilder = new ObservationBuilder();
        String id = observation.getId().getValue();
        String status = observation.getStatus().getValue();

        List<Serializable> codes = observation.getCode().getContent();
        boolean snomedFound = false;
        for (int i = 0; i < codes.size(); i++) {
            if (null != codes.get(i)) {
                if (codes.get(i) instanceof String) {
                } else if (codes.get(i) instanceof JAXBElement) {
                    JAXBElement<CodingType> coding = (JAXBElement<CodingType>) codes.get(i);
                    CodingType converted = coding.getValue();
                    // coded concept - these are Snomed coded
                    if (converted.getSystem().getValue().equals(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
                        snomedFound = true;
                        String code = converted.getCode().getValue();

                        CodeableConceptBuilder codeableConceptBuilder
                                = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

                        if(code.equals("1097291000000101") || code.equals("237364002")){
                            String column = code.equals("1097291000000101") ? "suspected_congenital_abnormality" : "stillborn_indicator";
                            String scheme = code.equals("1097291000000101") ? IMConstant.FHIR_SCAI : IMConstant.FHIR_SBI;
                            String fhirCoding = code.equals("1097291000000101") ? FhirCodeUri.CODE_SYSTEM_FHIR_SUSPECTED_CONGENITAL_ABNORMALITY_INDICATOR : FhirCodeUri.CODE_SYSTEM_FHIR_STILLBORN_INDICATOR;
                                    String value = observation.getValueCodeableConcept().getCoding().getCode().getValue();
                            codeableConceptBuilder.addCoding(fhirCoding);
                            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                                    "CM_Org_NHS", "CM_Sys_NEMS", "FHIR", "PDS", column, value, scheme);
                            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);
                            codeableConceptBuilder.setCodingCode(valueResponse.getConcept().getCode());
                        }
                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                        codeableConceptBuilder.setCodingCode(code);

                        //only the code is supplied so perform a lookup to derive the term
                        String observationTerm = TerminologyService.lookupSnomedTerm(code);
                        codeableConceptBuilder.setCodingDisplay(observationTerm);
                        codeableConceptBuilder.setText(observationTerm);
                    }
                }
            }
        }

        if (!snomedFound) {
            throw new TransformException("OBSERVATION PROVIDED WITHOUT SNOMED CODE OBS ID: " + id);
        }

        if (null != observation.getValueQuantity()) {
            if (null != observation.getValueQuantity().getValue()) {
                String value = observation.getValueQuantity().getValue().getValue();
                observationBuilder.setValueNumber(Double.parseDouble(value));
            }
            if (null != observation.getValueQuantity().getUnit()) {
                String units = observation.getValueQuantity().getUnit().getValue();

                observationBuilder.setValueNumberUnits(units);
            }
        }
        String code = "";
        String system = "";
        String term = "";
        if (null != observation.getCategory()) {
            CodingType categoryCoding = observation.getCategory().getCoding();
            if (null != categoryCoding.getCode()) {
                code = observation.getCategory().getCoding().getCode().getValue();
            }
            if (null != categoryCoding.getSystem()) {
                system = observation.getCategory().getCoding().getSystem().getValue();
            }
            if (null != categoryCoding.getDisplay()) {
                term = observation.getCategory().getCoding().getDisplay().getValue();
            }
        }
        CodeableConcept cat = CodeableConceptHelper.createCodeableConcept(code, system, term);
        observationBuilder.setCategory(cat);



        String patientId = xmlHelper.purifyUUID(observation.getSubject().getReference().getValue());
        PatientBuilder patient = patientMap.get(patientId);
        Reference patientRef = xmlHelper.createPatientReference(patient.getResourceId());

        if(null != patient) {

            observationBuilder.setPatient(patientRef);

            EncounterBuilder encounter = getEncounterBuilder(patient.getResourceId(), xmlHelper);
            if(null != encounter)
                observationBuilder.setEncounter(xmlHelper.createEncounterReference(encounter.getResourceId()));

        } else {
            throw new TransformException("Could not set patient for observation " + id +
                    " as no patient with ID " + patient.getResourceId() + " could be found");
        }

        if(practitioners.size() > 0) {
            String practitionerId = practitioners.get(0);
            Reference practitionerReference = xmlHelper.createPractitionerReference(practitionerId);
            observationBuilder.setClinician(practitionerReference);
        }

        DateTimeType date = new DateTimeType(birthDate);
        observationBuilder.setEffectiveDate(date);

        String obsID = xmlHelper.createUniqueId(id, ResourceType.Observation);
        observationBuilder.setId(obsID);
        observationBuilder.setStatus(Observation.ObservationStatus.fromCode(status));

        observations.add(observationBuilder);
    }

    static void processRelatedPerson(RelatedPersonType relatedPerson, NemsXmlHelper xmlHelper) throws Exception {
        if(linkRelatedPersonIdToMother.get(relatedPerson.getId().getValue()) != null) {
            PatientBuilder motherBuilder = linkRelatedPersonIdToMother.get(relatedPerson.getId().getValue());
            String babyRawId = relatedPerson.getPatient().getReference().getValue();
            PatientBuilder babyBuilder = patientMap.get(xmlHelper.purifyUUID(babyRawId));

            linkedBabyAndMotherIds.put(babyBuilder.getResourceId(), motherBuilder.getResourceId());

            Reference motherReference = xmlHelper.createPatientReference(motherBuilder.getResourceId());
            addNaturalMother(babyBuilder, motherReference);
        }
    }

    static void addNaturalMother(PatientBuilder patientBuilder, Reference reference) {
        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(patientBuilder);
        containedParametersBuilder.removeContainedParameters();
        containedParametersBuilder.addParameter("NMTH", reference);
    }

    //use NHS number instead of this other dumb number
    static void processPatient(PatientType patient, NemsXmlHelper xmlHelper) throws Exception {
        if(patient.getIdentifier() != null) {
            Patient fhirPatient = null; //= (Patient) retrieveResource(createUniqueId(patient.getIdentifier().getValue().getValue(), ResourceType.Patient), ResourceType.Patient);

            PatientBuilder patientBuilder = new PatientBuilder();

            if (fhirPatient != null) {
                patientBuilder = new PatientBuilder(fhirPatient);
            }

            boolean isBaby = true;
            if (patient.getLink() != null) {
                isBaby = false;
            }

            if (fhirPatient == null) {
                patientBuilder.setId(xmlHelper.createUniqueId(patient.getIdentifier().getValue().getValue(), ResourceType.Patient));
                if (null != patient.getBirthDate()) {
                    xmlHelper.createDOB(patientBuilder, patient.getBirthDate(), isBaby);
                }

                if(null != patient.getDeceasedDateTime()){
                    xmlHelper.createDOD(patientBuilder, patient.getDeceasedDateTime());
                }

                xmlHelper.createName(patientBuilder, patient.getName());
                xmlHelper.createGender(patientBuilder, patient);

                IdentifierType nationalNumberType = patient.getIdentifier();
                if (null != nationalNumberType) {
                    xmlHelper.createIdentifier(patientBuilder, Identifier.IdentifierUse.OFFICIAL, nationalNumberType.getExtension().getValueCodeableConcept().getCoding().getCode().getValue(), patient.getIdentifier());

                }

                AddressType address = patient.getAddress();
                if (null != address) {
                    xmlHelper.createAddress(patientBuilder, address);
                }

                //Patient will sometimes have >1 extension, so need to find the right one for Ethnicity
                ExtensionType ethnicExtension
                        = xmlHelper.findPatientExtension(patient, "https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect-EthnicCategory-1");
                if (null != ethnicExtension) {
                    EthnicCategory category = xmlHelper.convertEthnicCategory(ethnicExtension.getValueCodeableConcept().getCoding().getCode().getValue());
                    patientBuilder.setEthnicity(category);
                }
            }

            GeneralPractitionerType registration = patient.getGeneralPractitioner();
            if (null != registration) {
                String odsCode = registration.getReference().getValue().substring(57);
                if (!Strings.isNullOrEmpty(odsCode)) {

                    Reference orgRef = xmlHelper.createOrganisationReference(xmlHelper.createUniqueId(odsCode, ResourceType.Organization));

                    patientBuilder.addCareProvider(orgRef);
                }
            }
            Reference managingOrg = xmlHelper.createOrganisationReference(xmlHelper.createUniqueId(NEMS_ODS, ResourceType.Organization));
            patientBuilder.setManagingOrganisation(managingOrg);

            LinkType babyLink = patient.getLink();
            if (null != babyLink) {
                linkRelatedPersonIdToMother.put(xmlHelper.purifyUUID(babyLink.getOther().getReference().getValue()), patientBuilder);
            }
            patientArray.add(patientBuilder);
            patientMap.put(xmlHelper.purifyUUID(patient.getId().getValue()), patientBuilder);

            //only set the UUID to NHS number link if it is available
            if (null != patient.getIdentifier()) {
                xmlHelper.saveInternalId(xmlHelper.NEMS_BIRTH_UUID_TO_NHS_NO, patient.getIdentifier().getValue().getValue(), patientBuilder.getResourceId());
            }
            LOG.info("Patient Processed");
        } else {
            TransformWarnings.log(LOG, xmlHelper, "skipping patient with id " + patient.getId().getValue() + "as they have no NHS number");
        }
    }

    public static void setEpisodeReferenceOnEncounter(String episodeId, EncounterBuilder encounterBuilder, NemsXmlHelper xmlHelper) throws Exception {
        Reference episodeReference = xmlHelper.createEpisodeReference(xmlHelper.createUniqueId(episodeId, ResourceType.EpisodeOfCare));
        encounterBuilder.setEpisodeOfCare(episodeReference);
        encounterBuilder.setId(xmlHelper.createUniqueId(episodeId, ResourceType.Encounter));
    }


    private static EncounterBuilder getEncounterBuilder(String patientId, NemsXmlHelper xmlHelper) throws Exception{

        if(null == patientId){
            throw new TransformException("Can not get Encounter. Patient reference is null");
        }

        if (null != encounterMap.get(patientId)) {
            return encounterMap.get(patientId);
        } else {
            return null;
        }

    }
    private static EncounterBuilder createEncounterBuilder(String patient, String episodeId, NemsXmlHelper xmlHelper) throws Exception{

        Reference patientRef = xmlHelper.createPatientReference(patient);

        if(null == patientRef){
            throw new TransformException("Can not get Encounter. Patient reference is null");
        }

        EncounterBuilder builder = new EncounterBuilder();
        builder.setId(xmlHelper.createUniqueId(episodeId, ResourceType.Encounter));
        builder.setPatient(patientRef);
        encounterMap.put(patient, builder);
        return builder;
    }

    private static void buildEpisodeOfCare(String patientId, String managingOrgID, String episodeId, boolean isBaby, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {
        EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder();
        builder.setId(xmlHelper.createUniqueId(episodeId, ResourceType.EpisodeOfCare));
        builder.setManagingOrganisation(xmlHelper.createOrganisationReference(managingOrgID));

        builder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        Reference patientRef = xmlHelper.createPatientReference(patientId);
        builder.setPatient(patientRef);
        builder.setRegistrationType(RegistrationType.MATERNITY_SERVICES);
        builder.setRegistrationStartDate(birthDate);
        episodes.add(builder);

        EncounterBuilder encounter = createEncounterBuilder(patientId, episodeId, xmlHelper);

        setEpisodeReferenceOnEncounter(episodeId, encounter, xmlHelper);
        encounter.setServiceProvider(xmlHelper.createOrganisationReference(managingOrgID));

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(encounter, CodeableConceptBuilder.Tag.Encounter_Source);
        if(isBaby) {
            codeableConceptBuilder.setText("Inpatient Birth");
        } else {
            codeableConceptBuilder.setText("Inpatient Delivery");
        }

        if(practitioners.size() > 0) {
            String practitionerId = practitioners.get(0);
            Reference practitionerReference = xmlHelper.createPractitionerReference(practitionerId);
            //encounter.setRecordedBy(practitionerReference);
            encounter.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        encounter.setPeriodStart(birthDate);
        encounters.add(encounter);
    }

    private static void processEpisodeOfCare(EpisodeOfCareType episode, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {
        String rawEpisodeId = episode.getId().getValue();
        String managingOrgID = organisationMap.get(xmlHelper.purifyUUID(episode.getManagingOrganization().getReference().getValue()));
        String patientId = xmlHelper.purifyUUID(episode.getPatient().getReference().getValue());
        PatientBuilder patient = patientMap.get(patientId);
        if(patient == null) {
            throw new TransformException("Can not create Patient Reference. Episode of Care: " + episode.getId().getValue() +
                    " does not contain Patient ID: " + episode.getPatient().getReference().getValue());
        }
        buildEpisodeOfCare(patient.getResourceId(), managingOrgID, rawEpisodeId, true, processor, xmlHelper);
        String motherId = linkedBabyAndMotherIds.get(patient.getResourceId());
        if(encounterMap.get(motherId) == null){
            //When creating a new episode of care from scratch, use the existing EOC ID and append 1 to it to keep it unique.
            if(linkedBabyAndMotherIds.get(patient.getResourceId()) != null) {
                buildEpisodeOfCare(linkedBabyAndMotherIds.get(patient.getResourceId()), managingOrgID, rawEpisodeId + "1",false, processor, xmlHelper);
            }
        }
    }

    private static void savePatients(FhirResourceFiler processor) throws Exception {
        for (PatientBuilder patient : patientArray) {
            processor.savePatientResource(null, patient);
        }
    }
    private static void saveEpisodesOfCare(FhirResourceFiler processor) throws Exception {
        for (EpisodeOfCareBuilder episode : episodes) {

            processor.savePatientResource(null, episode);
        }
    }
    private static void saveEncounters(FhirResourceFiler processor) throws Exception {
        for (EncounterBuilder encounter : encounters) {

            processor.savePatientResource(null, encounter);
        }
    }
    private static void saveObservations(FhirResourceFiler processor) throws Exception {
        for (ObservationBuilder observation : observations) {

            processor.savePatientResource(null, observation);
        }
    }
}

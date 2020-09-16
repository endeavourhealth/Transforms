package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.segment.MRG;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceMergeMapHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.fhirhl7v2.FhirHl7v2Filer;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    /**
     *
     * @param pid
     * @param patientBuilder
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @return
     * @throws Exception
     */
    public static PatientBuilder transformPIDToPatient(PID pid, PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper) throws Exception {
        CX[] patientIdList = pid.getPatientIDInternalID();
        String id = String.valueOf(patientIdList[0].getID());
        String nhsNumber = String.valueOf(patientIdList[1].getID());
        createIdentifier(patientBuilder, fhirResourceFiler, nhsNumber, Identifier.IdentifierUse.OFFICIAL, "https://fhir.hl7.org.uk/Id/nhs-number");

        //store the patient ID to the patient resource
        createIdentifier(patientBuilder, fhirResourceFiler, id, Identifier.IdentifierUse.SECONDARY, "https://fhir.hl7.org.uk/Id/ryj");

        TS dob = pid.getDateOfBirth();
        if (!dob.isEmpty()) {
            String dtB = String.valueOf(dob.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtB.substring(0,4)+"-"+dtB.substring(4,6)+"-"+dtB.substring(6,8));
            patientBuilder.setDateOfBirth(date);
        }

        TS dod = pid.getPatientDeathDateAndTime();
        if (!dod.isEmpty()) {
            String dtD = String.valueOf(dob.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtD.substring(0,4)+"-"+dtD.substring(4,6)+"-"+dtD.substring(6,8));
            patientBuilder.setDateOfDeath(date);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        IS sex = pid.getSex();
        VocSex sexEnum = VocSex.fromValue(String.valueOf(sex));
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender);

        createName(patientBuilder, pid, fhirResourceFiler);
        createAddress(patientBuilder, pid, fhirResourceFiler);

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        return patientBuilder;
    }

    /**
     * A34 messages merge all content from one patient (minor patient) to another (the major patient)
     */
    public static void performA34PatientMerge(FhirHl7v2Filer.AdtResourceFiler filer, PID pid, MRG mrg) throws Exception {

        CX[] patientIdList = pid.getPatientIDInternalID();
        String majorPId = String.valueOf((mrg.getPriorPatientIDInternal())[0].getID());
        String minorPId = String.valueOf(patientIdList[0].getID());

        UUID majPatientId = IdHelper.getEdsResourceId(filer.getServiceId(), ResourceType.Patient, majorPId);
        String majorPatientId = String.valueOf(majPatientId);

        UUID minPatientId = IdHelper.getEdsResourceId(filer.getServiceId(), ResourceType.Patient, minorPId);
        String minorPatientId = String.valueOf(minPatientId);

        LOG.debug("Doing A34 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = new HashMap<>();

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId);
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId);
        idMappings.put(minorPatientReference, majorPatientReference);

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(filer.getServiceId(), UUID.fromString(minorPatientId));

        //copy the resources to the major patient
        for (ResourceWrapper minorPatientResource: minorPatientResources) {

            String json = minorPatientResource.getResourceData();
            Resource fhirOriginal = ParserPool.getInstance().parse(json);

            if (fhirOriginal instanceof Patient) {
                //we don't want to move patient resources, so just delete it
                filer.deletePatientResource(new GenericBuilder(fhirOriginal));

            } else {
                //for all other resources, re-map the IDs and save to the DB
                try {
                    //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
                    IdHelper.applyExternalReferenceMappings(fhirOriginal, idMappings, false);
                    filer.savePatientResource(new GenericBuilder(fhirOriginal));

                } catch (Exception ex) {
                    throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + minorPatientResource.getResourceId() + " and now has " + fhirOriginal.getId(), ex);
                }

                LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId());
            }
        }

        //need to wait here until all resources have been saved, otherwise if we let the below fn save the resource
        //mappings (of old patient to new patient) then the mapping gets applied when deleting the old patient (since
        //the delete is in a different thread). End result is that we end up KEEPING the minor patient and DELETING
        //the one we want to keep!
        filer.waitUntilEverythingIsSaved();

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(filer.getServiceId(), idMappings);
    }

    private static Parameters findParameters(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.Parameters) {
                return (Parameters)resource;
            }
        }

        throw new TransformException("Failed to find Parameters resource in Bundle");
    }

    private static String findParameterValue(Parameters parameters, String name) throws Exception {
        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase(name)) {
                    StringType value = (StringType)component.getValue();
                    return value.getValue();
                }
            }
        }

        throw new TransformException("Failed to find parameter [" + name + "] in Parameters resource");
    }

    //returns a map of old Ids to new, formatted as FHIR references (e.g. Patient/<guid>)
    private static Map<String, String> findIdMappings(Parameters parameters) throws Exception {

        Map<String, String> referenceIdMap = new HashMap<>();

        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase("OldToNewResourceMap")) {

                    for (Parameters.ParametersParameterComponent part: component.getPart()) {
                        String name = part.getName();
                        String value = ((StringType)part.getValue()).toString();

                        referenceIdMap.put(name, value);
                    }
                }
            }
        }

        LOG.debug("Id mappings are");
        for (String key: referenceIdMap.keySet()) {
            String value = referenceIdMap.get(key);
            LOG.debug(key + " -> " + value);
        }

        return referenceIdMap;
    }

    /**
     *
     * @param patientBuilder
     * @param fhirResourceFiler
     * @param cell
     * @param use
     * @param system
     * @throws Exception
     */
    private static void createIdentifier(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, String cell,
                                         Identifier.IdentifierUse use, String system) throws Exception {
        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(use);
            identifierBuilder.setSystem(system);
            identifierBuilder.setValue(cell);

            //make sure to de-duplicate the identifier if we've just added a new instance with the same value
            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            //if cell is empty, make sure to end any previous records
            IdentifierBuilder.endIdentifiers(patientBuilder, fhirResourceFiler.getDataDate(), system, use);
        }
    }

    /**
     *
     * @param patientBuilder
     * @param pid
     * @param fhirResourceFiler
     * @throws Exception
     */
    private static void createAddress(PatientBuilder patientBuilder, PID pid, FhirResourceFiler fhirResourceFiler) throws Exception {

        XAD[] patientAddress = pid.getPatientAddress();
        if (patientAddress != null && patientAddress.length > 0) {

            ST streetAddress = patientAddress[0].getStreetAddress();
            ST city = patientAddress[0].getCity();
            ST state = patientAddress[0].getStateOrProvince();
            ST postcode = patientAddress[0].getZipOrPostalCode();
            ID country = patientAddress[0].getCountry();

            if (!streetAddress.isEmpty()
                    || !city.isEmpty()
                    || !state.isEmpty()
                    || !postcode.isEmpty()
                    || !country.isEmpty()) {

                AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
                addressBuilder.setUse(Address.AddressUse.HOME);
                addressBuilder.addLine(String.valueOf(streetAddress));
                addressBuilder.addLine(String.valueOf(country));
                addressBuilder.setCity(String.valueOf(city));
                addressBuilder.setDistrict(String.valueOf(state));
                addressBuilder.setPostcode(String.valueOf(postcode));

                AddressBuilder.deDuplicateLastAddress(patientBuilder, fhirResourceFiler.getDataDate());

            } else {
                AddressBuilder.endAddresses(patientBuilder, fhirResourceFiler.getDataDate(), Address.AddressUse.HOME);
            }
        }
    }

    /**
     *
     * @param patientBuilder
     * @param pid
     * @param fhirResourceFiler
     * @throws Exception
     */
    private static void createName(PatientBuilder patientBuilder, PID pid, FhirResourceFiler fhirResourceFiler) throws Exception {
        XPN[] patientName = pid.getPatientName();
        ST familyName = patientName[0].getFamilyName();
        ST givenName = patientName[0].getGivenName();
        ID nameTypeCode = patientName[0].getNameTypeCode();
        ST prefix = patientName[0].getPrefixEgDR();

        if (!prefix.isEmpty() || !givenName.isEmpty() || !familyName.isEmpty()) {
            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.addPrefix(String.valueOf(prefix));
            nameBuilder.addGiven(String.valueOf(givenName));
            nameBuilder.addFamily(String.valueOf(familyName));

            NameBuilder.deDuplicateLastName(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            NameBuilder.endNames(patientBuilder, fhirResourceFiler.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }
}

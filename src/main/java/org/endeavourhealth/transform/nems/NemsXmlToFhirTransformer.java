package org.endeavourhealth.transform.nems;

import org.apache.commons.io.IOUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.XmlHelper;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.nems.birthNotification.admin.OrganisationTransformer;
import org.endeavourhealth.transform.nems.birthNotification.admin.PatientBirthTransformer;
import org.endeavourhealth.transform.nems.birthNotification.admin.PractitionerTransformer;
import org.endeavourhealth.transform.nems.deathNotification.admin.PatientDeathTransformer;
import org.endeavourhealth.transform.nems.schema.v1.BundleType;
import org.endeavourhealth.transform.nems.schema.v1.EntryType;
import org.endeavourhealth.transform.nems.schema.v1.PatientType;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class NemsXmlToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(NemsXmlToFhirTransformer.class);
    public static HashMap<String, String> uniqueIdMapper = new HashMap<String, String>();

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static void transform(String exchangeBody, FhirResourceFiler processor, String version) throws Exception {
        NemsXmlHelper xmlHelper = new NemsXmlHelper();
        xmlHelper.setServiceId(processor.getServiceId());
        xmlHelper.setExchangeId(processor.getExchangeId());
        xmlHelper.setSystemId(processor.getSystemId());
        //the exchange body will be a list of files received from NEMS, typically one XML .dat file per exchange
        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);

        if (files.length == 0) {
            LOG.warn("Exchange {} file list is empty for service {} - skipping", processor.getExchangeId().toString(), processor.getServiceId().toString());
            return;
        }

        //files should all be in a directory structure of org folder -> processing ID folder -> XML .dat file
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //read the XML from each exchange file
        LOG.info("Invoking NEMS XML transformer for " + files.length + " file(s) using service " + processor.getServiceId());
        for (String exchangeFile : files) {

            InputStream streamXML = FileHelper.readFileFromSharedStorage(exchangeFile);
            String xml = IOUtils.toString(streamXML, StandardCharsets.UTF_8.name());

            try {
                if (xml.contains(xmlHelper.FILE_ID_BIRTH_NOTIFICATION)) {

                    LOG.trace("Transforming NEMS XML file {} content in {}", exchangeFile, orgDirectory);
                    transformBirthNotification(xml, processor, xmlHelper);

                } else if (xml.contains(xmlHelper.FILE_ID_DEATH_NOTIFICATION)) {

                    LOG.trace("Transforming NEMS XML file {} content in {}", exchangeFile, orgDirectory);
                    transformDeathNotification(xml, processor, xmlHelper);
                } else {

                    throw new FileFormatException(exchangeFile, "Unexpected file " + exchangeFile + " in NEMS XML exchange");
                }
            } finally {
                streamXML.close();
            }
        }
    }

    static void transformBirthNotification(String xml, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {
        xmlHelper.validateBatch(true);
        BundleType birthNotification = XmlHelper.deserialize(xml, BundleType.class);

        for(EntryType entry : birthNotification.getEntry()){
            PatientType p = entry.getResource().getPatient();
            if(null != p){
                if(p.getIdentifier()== null){

                    NemsXmlHelper.validateBatch(false);
                    TransformWarnings.log(LOG, xmlHelper, "Patient with extract ID {} does not have an NHS number, skipping this message", p.getId().getValue());
                    return;
                }
            }
        }

        OrganisationTransformer.transform(birthNotification, processor, xmlHelper);
                /*
                    --> extract, create and file Organisation FHIR resources using ODS lookup
                    --> cache the main delivery Hospital reference to assign to the encounters later
                 */

        PractitionerTransformer.transform(birthNotification, processor, xmlHelper);
                 /*
                    --> extract, create and file Practitioner FHIR resource
                    --> cache this reference to assign to encounters and observations later
                 */

        PatientBirthTransformer.transform(birthNotification, processor, xmlHelper);
                /*
                    --> extract, create and file the Mother Patient FHIR resource and the Baby FHIR resource(s)
                    --> create and associate an EpisodeOfCare FHIR resource for each Patient resource
                    --> process and file the RelatedPerson record between Mother and Baby(s)
                        NOTE: this will require a new RelatedPerson FHIR Resource builder and subscriber filer into patient_additional
                    --> create and file an Encounter FHIR resource for each patient at the ServiceProviderOrganization = Hospital Organization
                    --> extract, create and file ALL the Observation FHIR records using the ID references for Mother or Baby
                 */
    }

    static void transformDeathNotification(String xml, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {
        BundleType deathNotification = XmlHelper.deserialize(xml, BundleType.class);
        PatientDeathTransformer.transform(deathNotification, processor, xmlHelper);
                /*
                    --> extract patient identifier and use to populate and locate an existing Patient FHIR record for the service
                    --> if Patient does not exist, log as a transform warning (unable to lookup patient ....)
                    --> if Patient does exist, set the deceasedDateTime, get and end the associated EpisodeOfCare
                           and file the updated Patient FHIR resource
                 */
    }

}
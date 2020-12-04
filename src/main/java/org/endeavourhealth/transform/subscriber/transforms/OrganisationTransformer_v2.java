package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.UPRN;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.endeavourhealth.transform.subscriber.IMConstant;

import org.endeavourhealth.core.database.rdbms.enterprise.EnterpriseConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.*;
import java.util.*;
import OpenPseudonymiser.Crypto;

public class OrganisationTransformer_v2 extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Organization;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        String configName = params.getSubscriberConfigName();

        audit("in transform", subscriberId.getSubscriberId(), configName);

        //String zid = GetLastInsertId();

        org.endeavourhealth.transform.subscriber.targetTables.Organization model = params.getOutputContainer().getOrganisations();
        org.endeavourhealth.transform.subscriber.targetTables.Organization_v2 model_v2 = params.getOutputContainer().getOrganisations_v2();
        org.endeavourhealth.transform.subscriber.targetTables.Location_v2 location_model_v2 = params.getOutputContainer().getLocation_v2();
        org.endeavourhealth.transform.subscriber.targetTables.AbpAddress_v2 abp_model_v2 = params.getOutputContainer().getAbpAddress_v2();
        org.endeavourhealth.transform.subscriber.targetTables.Property_v2 property_model_v2 = params.getOutputContainer().getProperty_v2();
        org.endeavourhealth.transform.subscriber.targetTables.Address_v2 address_model_v2 = params.getOutputContainer().getAddress_v2();
        org.endeavourhealth.transform.subscriber.targetTables.UprnMatchEvent_v2 uprn_match_event_model_v2 = params.getOutputContainer().getUprnMatchEvent_v2();
        org.endeavourhealth.transform.subscriber.targetTables.OrganizationAdditional org_additional_model_v2 = params.getOutputContainer().getOrganizationAdditional();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        Organization fhir = (Organization)resourceWrapper.getResource();

        String odsCode = null;
        String name = null;
        String typeCode = null;
        String typeDesc = null;
        String postcode = null;
        Long parentOrganisationId = null;

        //OrganizationAdditional cqc stuff ** no longer needed
        /*
        String cqc_id = null; String services_web_site = null; String service_type = null;
        String date_of_last_check = null; String specialism = null;
        String provider_name = null; String local_authority = null;
        String location_url = null; String cqc_location = null;
        String cqc_provider_id = null; String on_ratings = null;
        String open_date = null; String close_date = null;
        String cqc_region = null; String cqc_address = null;
        String cqc_postcode = null;
         */

        if (fhir.hasIdentifier()) {
            odsCode = IdentifierHelper.findIdentifierValue(fhir.getIdentifier(), FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        }

        //we have at least one Emis org without a name, which is against their spec, but we need to handle it
        if (fhir.hasName()) {
            name = fhir.getName();
        } else {
            name = "";
        }

        if (fhir.hasPartOf()) {
            Reference partOfReference = fhir.getPartOf();
            parentOrganisationId = transformOnDemandAndMapId(partOfReference, SubscriberTableId.ORGANIZATION, params);
        }

        if (fhir.hasType()) {
            CodeableConcept cc = fhir.getType();
            for (Coding coding: cc.getCoding()) {
                if (coding.getSystem().equals(FhirValueSetUri.VALUE_SET_ORGANISATION_TYPE)) {
                    typeCode = coding.getCode();
                    typeDesc = coding.getDisplay();
                }
            }
        }

        if (fhir.hasContained()) {
            for (Resource containedResource: fhir.getContained()) {
                Parameters parameters = (Parameters)containedResource;
                List<Parameters.ParametersParameterComponent> entries = parameters.getParameter();
                for (Parameters.ParametersParameterComponent parameter : entries) {
                    if (parameter.hasName() && parameter.hasValue()) {
                        String propertyCode = parameter.getName();
                        String propertyScheme = IMConstant.DISCOVERY_CODE;
                        if (!propertyCode.startsWith("JSON_")) {
                            String type = parameter.getValue().getClass().getSimpleName();
                            if (type.equalsIgnoreCase("CodeableConcept")) {
                                CodeableConcept parameterValue = (CodeableConcept) parameter.getValue();

                                String valueCode = parameterValue.getCoding().get(0).getCode();
                                String valueScheme = parameterValue.getCoding().get(0).getSystem();
                                String valueDisplay = parameterValue.getCoding().get(0).getDisplay();
                                String[] ss = valueDisplay.split("\\~",-1);
                                String vDisplay = ss[0]; String vName = ss[1];

                                Integer propertyConceptDbid = FindDBID(propertyScheme, propertyCode, configName);
                                Integer valueConceptDbid = FindDBID(valueScheme, valueCode, configName);

                                if (propertyConceptDbid.equals(0)) {
                                    propertyConceptDbid = IMClient.getConceptDbidForSchemeCode(propertyScheme, propertyCode);
                                    InsertDBID(propertyScheme, propertyCode, propertyConceptDbid, configName);
                                }

                                if (valueConceptDbid.equals(0)) {
                                    valueConceptDbid = IMClient.getConceptDbidForSchemeCode(valueScheme, valueCode);
                                    InsertDBID(valueScheme, valueCode, valueConceptDbid, configName);
                                }
                                org_additional_model_v2.writeUpsert(subscriberId, propertyConceptDbid, valueConceptDbid, "", vDisplay, vName);
                            }
                        }
                    }
                }
            }
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.ORGANISATION_MAIN_LOCATION)) {

                    Reference locationReference = (Reference)extension.getValue();

                    Location location = (Location)params.findOrRetrieveResource(locationReference);
                    if (location == null) {
                        //The Emis data contains organisations that refer to organisations that don't exist
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to " + locationReference.getReference() + " that doesn't exist");
                        continue;
                    }

                    if (location != null
                            && location.hasAddress()) {
                        Address address = location.getAddress();
                        if (address.hasPostalCode()) {
                            postcode = address.getPostalCode();
                        }
                    }
                }

            }
        }

        //to align the target DB with TRUD, use the ODS code to find the official name, parent etc. so the
        //DB doesn't end up with whatever weirdness came from Emis, TPP etc.
        if (!Strings.isNullOrEmpty(odsCode)) {
            OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(odsCode);
            if (odsOrg != null) {
                if (odsOrg.getOrganisationName() != null) {
                    name = odsOrg.getOrganisationName();
                }

                OrganisationType odsType = null;

                Set<OrganisationType> types = new HashSet<>(odsOrg.getOrganisationTypes());
                types.remove(OrganisationType.PRESCRIBING_COST_CENTRE); //always remove so we match to the "better" type
                if (types.size() == 1) {
                    odsType = types.iterator().next();
                } else {
                    LOG.warn("Could not select type for org " + odsOrg);
                }

                if (odsType != null) {
                    typeCode = odsType.getCode();
                    typeDesc = odsType.getDescription();
                }

                if (odsOrg.getPostcode() != null) {
                    postcode = odsOrg.getPostcode();
                }
            }
        }

        List<EnterpriseConnector.ConnectionWrapper> connectionWrappers = EnterpriseConnector.openSubscriberConnections(configName);
        EnterpriseConnector.ConnectionWrapper connectionWrapper = connectionWrappers.get(0);

        String organization_address_lines = "";

        String[] bits = connectionWrapper.toString().split("/");
        String schema = bits[bits.length-1];

        String contact_id = "";
        if (fhir.hasTelecom()) {
            org.endeavourhealth.transform.subscriber.targetTables.OrganizationContact_v2 contact_model = params.getOutputContainer().getOrganizationContact_v2();

            for (ContactPoint contact: fhir.getTelecom()) {
                String contact_type = contact.getSystem().toString();
                String value = contact.getValue();

                // get contact_id
                // select id from contact where id = subscriberId and value = value

                if (value !=null && !value.isEmpty()) {
                    String sql = "select id from organization_contact_v2 where organization_id=" + subscriberId.getSubscriberId() + " AND value='" + value + "'";
                    contact_id = getId(connectionWrapper, params, sql);
                    if (contact_id.isEmpty()) {
                        //sql = getNextIdSQL(schema, "organization_contact_v2");
                        //contact_id = getId(connectionWrapper, params, sql);
                        contact_id = GetLastInsertId(configName);
                    }
                    contact_model.writeUpsert(Long.parseLong(contact_id), subscriberId.getSubscriberId(), contact_type, value);
                }
            }
        }

            String adrec = ""; String address_line_1 = ""; String address_line_2 = ""; String address_line_3 = "";
            String city =""; String district = ""; String uprn_ralf00 = "";
            if (fhir.hasAddress()) {
                Address address = fhir.getAddress().get(0);
                adrec = address.getText().toString();
                if (address.hasPostalCode()) {postcode = address.getPostalCode();}
                if (address.hasDistrict()) {district = address.getDistrict();} // Ilford
                if (address.hasCity()) {city = address.getCity();};
                if (address.hasLine()) {
                    Integer c = 1;
                    for (StringType st: address.getLine()) {
                        if (c.equals(1)) {address_line_1=st.toString();}
                        if (c.equals(2)) {address_line_2=st.toString();}
                        if (c.equals(3)) {address_line_3=st.toString();}
                        ++c;
                    }
                }
            }

            String csv_address = address_line_1+"~"+address_line_2+"~"+address_line_3+"~"+city+"~"+district+"~"+postcode.replaceAll(" ","");
            csv_address = csv_address.toLowerCase();

            String abp_uprn = null;
            String abp_flat = null; String abp_building = null;
            String abp_number = null; String abp_throughfare = null;
            String abp_street = null; String abp_dep_locality = null;
            String abp_locality = null; String abp_town = null;
            String abp_postcode = null; String abp_organization = null;
            String abp_class_code = null;

            String sLatitude = null; String sLongitude = null;
            String point = null; String sX = null; String sY = null;

            String location_id = null; String sql = null; String property_id = null;
            String abp_address_id = null; String abp_class_id = "0";
            String project_ralf_id = "0"; // this might be a UUID in DSM?

            Boolean new_location;
            new_location = false;

            String address_id = ""; String uprn_match_event_id = "";

            BigDecimal latitude = new BigDecimal(0);
            BigDecimal longitude = new BigDecimal(0);
            BigDecimal x = new BigDecimal(0);
            BigDecimal y = new BigDecimal(0);

            String qualifier = ""; String algorithm = ""; String match_pattern_postcode = "";
            String match_pattern_street = ""; String match_pattern_number =""; String match_pattern_building = "";
            String match_pattern_flat = ""; String algorithm_version = ""; String epoch = "";
            String csv = ""; String addressv2 = "";

            if (UPRN.isActivated(configName)) {
                String ids = "org`"+Long.toString(subscriberId.getSubscriberId()) + "`" + configName;

                csv = UPRN.getAdrec(adrec, ids);
                if (Strings.isNullOrEmpty(csv)) {
                    System.out.println("Unable to get address from UPRN API");
                }

                audit(csv, subscriberId.getSubscriberId(), configName);

                //System.out.println("Got UPRN result " + csv);
                String[] ss = csv.split("\\~", -1);

                abp_uprn = ss[20]; abp_locality = ss[0]; abp_number = ss[1]; abp_organization = ss[2];

                if (abp_uprn.isEmpty()) {
                    System.out.println("UPRN = 0");
                    //return;
                }

                abp_postcode = ss[3]; abp_street = ss[4]; abp_town = ss[5]; sLatitude = ss[14]; sLongitude = ss[15];
                point = ss[16]; sX = ss[17]; sY = ss[18]; abp_class_code = ss[19];

                qualifier = ss[7]; algorithm = ss[6]; match_pattern_postcode = ss[11];
                match_pattern_street = ss[12]; match_pattern_number = ss[10];
                match_pattern_building = ss[8]; match_pattern_flat = ss[9];
                algorithm_version = "4.2"; epoch = "b";

                String losa_code = ""; String msoa_code = ""; String imp_code = "";

                if (params.isPseudonymised()) {uprn_ralf00 = pseudoUprn(abp_uprn, params);}

                /* no need to pseudonymise
                if (params.isPseudonymised()) {
                    uprn_ralf00 = pseudoUprn(abp_uprn, params);
                    abp_uprn = "";
                    // and null the rest of the fields
                    sLatitude = ""; sLongitude = ""; sX = ""; sY = "";
                    abp_flat = ""; abp_building = ""; abp_number = "";
                    abp_dep_locality = ""; abp_class_code = "";
                    abp_throughfare = ""; abp_town = ""; abp_street = "";
                }
                */

                // main_location_id
                sql = "SELECT location_id as id FROM organization_v2 ov2 where ov2.id="+subscriberId.getSubscriberId();
                location_id = getId(connectionWrapper, params, sql);
                if (!location_id.isEmpty()) {
                    // get the current address of the organization
                    addressv2 = getAddressv2(connectionWrapper, params, location_id);
                }

                new_location=true;
                // Check uprn and update the UPRN if changed
                if (!addressv2.isEmpty() && csv_address.equals(addressv2))
                {
                    // has abp_uprn changed?
                    String loc_uprn = getLatestUprn(connectionWrapper, params, location_id);
                    ss = loc_uprn.split("\\~", -1);
                    String zU=ss[0]; // uprn
                    if (zU.equals(abp_uprn)) {new_location=false;}
                }

                if (new_location.booleanValue() == true) {location_id = GetLastInsertId(configName);}

                /*
                // use the previous response from the api to decide if we create a new location?
                sql = "SELECT l.id FROM location_v2 l ";
                sql = sql + "join uprn_match_event_v2 um on um.location_id=l.id ";
                sql = sql + "where response='"+csv+"'";

                location_id = getId(connectionWrapper, params, sql);
                if (location_id.isEmpty()) {
                    new_location = true;
                    //sql = getNextIdSQL(schema, "location_v2");
                    //location_id = getId(connectionWrapper, params, sql);
                    location_id = GetLastInsertId();
                    System.out.println(location_id);
                }
                 */

                // address_v2 (address_id)
                sql = "SELECT id FROM address_v2 where location_id='"+location_id+"'";
                address_id = getId(connectionWrapper, params, sql);
                if (address_id.isEmpty()) {
                    address_id = GetLastInsertId(configName);
                }

                if (!sLatitude.isEmpty()) {
                    latitude = new BigDecimal(sLatitude);
                }

                if (!sLongitude.isEmpty()) {
                    longitude = new BigDecimal(sLongitude);
                }

                if (!sX.isEmpty()) {
                    x = new BigDecimal(sX);
                }

                if (!sY.isEmpty()) {
                    y = new BigDecimal(sY);
                }

                // location_v2 (uprn, uprn_ralf00, latitude, longitude, xcoordinate, ycoordinate, lsoa, msoa, imp)
                location_model_v2.writeUpsert(
                        Long.parseLong(location_id),
                        name,
                        typeCode,
                        typeDesc,
                        postcode,
                        subscriberId.getSubscriberId(),
                        abp_uprn,
                        uprn_ralf00,
                        latitude,
                        longitude,
                        x,
                        y,
                        losa_code,
                        msoa_code,
                        imp_code
                );

                // address_v2
                address_model_v2.writeUpsert(
                        Long.parseLong(address_id),
                        address_line_1,
                        address_line_2,
                        address_line_3,
                        city,
                        district,
                        postcode,
                        Long.parseLong(location_id)
                );
            }

            // abp_address_v2
            if (new_location.booleanValue() == true) {
                // new location means we need to populate property_v2, apb_address_v2 etc
                property_id = GetLastInsertId(configName);

                property_model_v2.writeUpsert(
                        Long.parseLong(property_id),
                        Long.parseLong(location_id),
                        Long.parseLong(project_ralf_id));

                abp_address_id = GetLastInsertId(configName);

                sql = "select id from abp_classification_v2 where code='"+abp_class_code+"'";
                abp_class_id = getId(connectionWrapper, params, sql);
                if (abp_class_id.isEmpty()) abp_class_id="0";

                abp_model_v2.writeUpsert(
                        Long.parseLong(abp_address_id),
                        Long.parseLong(property_id),
                        abp_flat,
                        abp_building,
                        abp_number,
                        abp_throughfare,
                        abp_street,
                        abp_dep_locality,
                        abp_locality,
                        abp_town,
                        abp_postcode,
                        abp_organization,
                        Long.parseLong(abp_class_id));
            }

            // audit trail (uprn_match_event_v2)
            uprn_match_event_id = GetLastInsertId(configName);

            String patient_address_id = "0";
            Date match_date = new Date();

            if (abp_address_id == null) {
                sql = "SELECT abp.id from property_v2 p ";
                sql = sql + "join abp_address_v2 abp on p.id=abp.property_id ";
                sql = sql + "where location_id='"+location_id+"'";
                abp_address_id = getId(connectionWrapper, params, sql);
            }

            // organization_v2
            model_v2.writeUpsert(subscriberId,
                odsCode,
                name,
                typeCode,
                typeDesc,
                postcode,
                parentOrganisationId,
                Long.parseLong(location_id));

            uprn_match_event_model_v2.writeUpsert(
                    Long.parseLong(uprn_match_event_id),
                    abp_uprn,
                    uprn_ralf00,
                    Long.parseLong(location_id),
                    Long.parseLong(patient_address_id),
                    latitude,
                    longitude,
                    x,
                    y,
                    qualifier,
                    algorithm,
                    match_date,
                    Long.parseLong(abp_address_id),
                    match_pattern_postcode,
                    match_pattern_street,
                    match_pattern_number,
                    match_pattern_building,
                    match_pattern_flat,
                    algorithm_version,
                    epoch,
                    addressv2 // previous_address
            );
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.ORGANIZATION_V2;
    }

    // defunct
    private String getNextIdSQL(String schema, String table)
    {
        // this is temporary - need to figure out a better way of getting the next id
        String sql = "SELECT AUTO_INCREMENT AS id FROM information_schema.TABLES ";
        sql = sql + "WHERE TABLE_SCHEMA = \""+schema+"\" ";
        sql = sql + "AND TABLE_NAME = \""+table+"\"";
        return sql;
    }

    public static String Test(OrganizationBuilder organizationBuilder) throws Exception
    {
        Resource resource = organizationBuilder.getResource();
        String json = FhirSerializationHelper.serializeResource(resource);
        return json;
    }

    public static void TestInsert() throws Exception
    {
        PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal("subscriber_test");
        //pseudoIdDal.PSTest();

        /*
        Connection connection = ConnectionManager.getSubscriberTransformConnection("subscriber_test");
        String sql = "insert into scratch_v2(cqc_id, guid) values('test1','test')";
        PreparedStatement preparedStmt = null;
        preparedStmt = connection.prepareStatement(sql);
        //preparedStmt.setString(1,cqc_id);
        //preparedStmt.setString(2,resource_guid);
        boolean z = preparedStmt.execute();
        System.out.println(z);
        preparedStmt.close();
        connection.close();
         */
    }

    private void audit(String text, Long subscriber_id, String configName) throws Exception
    {
        Connection connection = ConnectionManager.getSubscriberTransformConnection(configName);
        String sql = "insert into cqc_audit(subscriber_id, text) values(?,?);";
        PreparedStatement preparedStmt = null;
        preparedStmt = connection.prepareStatement(sql);
        preparedStmt.setLong(1, subscriber_id);
        preparedStmt.setString(2, text);
        preparedStmt.executeUpdate();
        connection.commit();
        preparedStmt.close();
        connection.close();
    }

    private void InsertDBID(String Scheme, String Code, Integer DBID, String configName) throws Exception
    {
        Connection connection = ConnectionManager.getSubscriberTransformConnection(configName);
        String sql = "insert into cqc_id_map(cqc_id, guid) values(?,?);";
        PreparedStatement preparedStmt = null;
        preparedStmt = connection.prepareStatement(sql);
        preparedStmt.setString(1,Scheme+"~"+Code);
        preparedStmt.setString(2,DBID.toString());
        preparedStmt.executeUpdate();
        connection.commit();
        preparedStmt.close();
        connection.close();
    }

    // returns an im dbid for scheme and code
    private Integer FindDBID(String Scheme, String Code, String configName) throws Exception
    {
        Connection connection = ConnectionManager.getSubscriberTransformConnection(configName);

        Code = Code.replaceAll("'","\\\\'");

        String sql = "select guid from cqc_id_map where cqc_id = '"+Scheme+"~"+Code+"'";

        PreparedStatement preparedStmt = connection.prepareStatement(sql);
        ResultSet rs = preparedStmt.executeQuery();
        Integer DBID = 0;
        if (rs.next()) {
            DBID = Integer.parseInt(rs.getString(1));
        }
        preparedStmt.close();
        connection.close();
        return DBID;

        /*
        String pathToCsv = "d:\\temp\\props.txt";
        String dbid = "";
        // read the file first to see if the property exists
        // if it does return it
        // otherwise append the property to the file
        File f = new File(pathToCsv);
        if (f.exists()) {
            String row;
            BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String csvScheme = data[0]; String csvCode = data[1];
                dbid = data[2];
                if (csvScheme.equals(Scheme) && csvCode.equals(Code)) {
                    return dbid;
                }
            }
        }
        return dbid;
         */
    }

    private String getLatestUprn(EnterpriseConnector.ConnectionWrapper connectionWrapper, SubscriberTransformHelper params, String main_location_id) throws Exception {
        String sql ="SELECT * FROM location_v2 where id="+main_location_id;
        Connection subscriberConnection = connectionWrapper.getConnection();
        PreparedStatement ps = subscriberConnection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        String uprn = ""; String uprn_ralf00="";
        if (rs.next()) {
            uprn = rs.getString("uprn");
            uprn_ralf00 = rs.getString("uprn_ralf00");
        }
        ps.close();
        subscriberConnection.close();
        if (uprn==null) uprn= "";
        if (uprn_ralf00==null) uprn_ralf00="";

        return uprn+"~"+uprn_ralf00;
    }

    private String pseudoUprn(String uprn, SubscriberTransformHelper params) throws Exception
    {
        String uprn_ralf00 = "";
        if (!uprn.isEmpty()) {
            SubscriberConfig c = params.getConfig();
            List<LinkDistributorConfig> salts = c.getRalfSalts();
            LinkDistributorConfig firstSalt = salts.get(0);
            String base64Salt = firstSalt.getSalt();
            byte[] saltBytes = Base64.getDecoder().decode(base64Salt);

            TreeMap<String, String> keys = new TreeMap<>();
            keys.put("UPRN", "" + uprn);

            Crypto crypto = new Crypto();
            crypto.SetEncryptedSalt(saltBytes);
            uprn_ralf00 = crypto.GetDigest(keys);
        }
        return uprn_ralf00;
    }

    private String getAddressv2(EnterpriseConnector.ConnectionWrapper connectionWrapper, SubscriberTransformHelper params, String main_location_id) throws Exception {
        String sql = "SELECT * FROM address_v2 where location_id = "+main_location_id+" order by id desc";
        Connection subscriberConnection = connectionWrapper.getConnection();
        PreparedStatement ps = subscriberConnection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        String address = "";
        String line_1=""; String line_2=""; String line_3=""; String city =""; String county=""; String postcode="";
        if (rs.next()) {
            if (rs.getString("line_1")!=null) {line_1=rs.getString("line_1");}
            if (rs.getString("line_2")!=null) {line_2 = rs.getString("line_2");}
            if (rs.getString("line_3")!=null) {line_3 = rs.getString("line_3");}
            if (rs.getString("city")!=null) {city = rs.getString("city");}
            if (rs.getString("county")!=null) {county = rs.getString("county");}
            if (rs.getString("postcode")!=null) {postcode = rs.getString("postcode");}
            address = line_1+"~"+line_2+"~"+line_3+"~"+city+"~"+county+"~"+postcode.replaceAll(" ","");
            address = address.toLowerCase();
        }
        ps.close();
        subscriberConnection.close();
        return address;
    }

    private String getId(EnterpriseConnector.ConnectionWrapper connectionWrapper, SubscriberTransformHelper params, String sql) throws Exception {
        {
            Connection subscriberConnection = connectionWrapper.getConnection();
            PreparedStatement ps = subscriberConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            String id = "";
            if (rs.next()) {
                id = rs.getString("id");
            }
            subscriberConnection.close();
            return id;
        }
    }

    private String GetLastInsertId(String configName) throws Exception
    {
        Connection connection = ConnectionManager.getSubscriberTransformConnection(configName);
        PreparedStatement ps = null;
        String sql = "INSERT INTO cqc_id_map (id) values (?)";
        PreparedStatement preparedStmt = connection.prepareStatement(sql);

        preparedStmt.setString(1,null);

        preparedStmt.execute();

        sql = "SELECT LAST_INSERT_ID()";
        preparedStmt = connection.prepareStatement(sql);
        ResultSet rs = preparedStmt.executeQuery();

        String id = "";
        if (rs.next()) {id = rs.getString("LAST_INSERT_ID()");}

        connection.close();
        return id;
    }
}

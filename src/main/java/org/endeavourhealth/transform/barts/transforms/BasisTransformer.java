package org.endeavourhealth.transform.barts.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.UUID;

public class BasisTransformer {
    private static Connection hl7receiverConnection = null;
    private static PreparedStatement resourceIdSelectStatement;
    private static PreparedStatement resourceIdInsertStatement;

    public static ResourceId getResourceId(String scope, String resourceType, String uniqueId) throws SQLException, ClassNotFoundException, IOException {
        //ResourceId resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
        ResourceId ret = null;
        if (hl7receiverConnection == null) {
            prepareResourceIdJDBC();
        }

        resourceIdSelectStatement.setString(1, scope);
        resourceIdSelectStatement.setString(2, resourceType);
        resourceIdSelectStatement.setString(3, uniqueId);

        ResultSet rs = resourceIdSelectStatement.executeQuery();
        if (rs.next()) {
            ret = new ResourceId();
            ret.setScopeId(scope);
            ret.setResourceType(resourceType);
            ret.setResourceId((UUID) rs.getObject(1));
        }
        rs.close();

        return ret;
    }


    public static void saveResourceId(ResourceId r) throws SQLException, ClassNotFoundException, IOException {
        if (hl7receiverConnection == null) {
            prepareResourceIdJDBC();
        }

        resourceIdInsertStatement.setString(1, r.getScopeId());
        resourceIdInsertStatement.setString(2, r.getResourceType());
        resourceIdInsertStatement.setString(3, r.getUniqueId());
        resourceIdInsertStatement.setObject(4, r.getResourceId());

        if (resourceIdInsertStatement.executeUpdate() != 1) {
            throw new SQLException("Could not create ResourceId:"+r.getScopeId()+":"+r.getResourceType()+":"+r.getUniqueId()+":"+r.getResourceId().toString());
        }
    }


    public static void prepareResourceIdJDBC() throws ClassNotFoundException, SQLException, IOException {
        JsonNode json = ConfigManager.getConfigurationAsJson("hl7receiver_db");

        Class.forName(json.get("drivername").asText());

        Properties connectionProps = new Properties();
        connectionProps.put("user", json.get("username").asText());
        connectionProps.put("password", json.get("password").asText());
        hl7receiverConnection = DriverManager.getConnection(json.get("url").asText(), connectionProps);

        resourceIdSelectStatement = hl7receiverConnection.prepareStatement("SELECT resource_uuid FROM mapping.resource_uuid where scope_id=? and resource_type=? and unique_identifier=?");
        resourceIdInsertStatement = hl7receiverConnection.prepareStatement("insert into mapping.resource_uuid (scope_id, resource_type, unique_identifier, resource_uuid) values (?, ?, ?, ?)");
    }

}

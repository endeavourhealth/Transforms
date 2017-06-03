
package org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Java class for AlertListType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="AlertListType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="Alert" type="{http://www.e-mis.com/emisopen/MedicalRecord}AlertType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AlertListType", namespace = "http://www.e-mis.com/emisopen/MedicalRecord", propOrder = {
    "alert"
})
public class AlertListType {

    @XmlElement(name = "Alert", namespace = "http://www.e-mis.com/emisopen/MedicalRecord")
    protected List<AlertType> alert;

    /**
     * Gets the value of the alert property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the alert property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getAlert().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link AlertType }
     * 
     * 
     */
    public List<AlertType> getAlert() {
        if (alert == null) {
            alert = new ArrayList<AlertType>();
        }
        return this.alert;
    }

}


package org.endeavourhealth.transform.tpp.xml.schema;

import javax.xml.bind.annotation.*;


/**
 * <p>Java class for FollowUpAction complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="FollowUpAction">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="Action" type="{}FollowUpActionType"/>
 *         &lt;element name="Comments" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FollowUpAction", propOrder = {
    "action",
    "comments"
})
public class FollowUpAction {

    @XmlElement(name = "Action", required = true)
    @XmlSchemaType(name = "string")
    protected FollowUpActionType action;
    @XmlElement(name = "Comments")
    protected String comments;

    /**
     * Gets the value of the action property.
     * 
     * @return
     *     possible object is
     *     {@link FollowUpActionType }
     *     
     */
    public FollowUpActionType getAction() {
        return action;
    }

    /**
     * Sets the value of the action property.
     * 
     * @param value
     *     allowed object is
     *     {@link FollowUpActionType }
     *     
     */
    public void setAction(FollowUpActionType value) {
        this.action = value;
    }

    /**
     * Gets the value of the comments property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getComments() {
        return comments;
    }

    /**
     * Sets the value of the comments property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setComments(String value) {
        this.comments = value;
    }

}

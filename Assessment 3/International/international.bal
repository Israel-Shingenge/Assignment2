import ballerinax/kafka;
import ballerina/io;
import International.sql_queries as sql_queries;

// Kafka consumer configuration to listen for shipment requests
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "international-consumer",
    topics: ["international-service"],
    pollingInterval: 1
};

// Kafka producer configuration to send processed responses
kafka:ProducerConfiguration producerConfig = {
    clientId: "international-producer",
    acks: "all",
    retryCount: 3
};

// Initialize Kafka producer
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfig);

// Define a listener to handle incoming messages
listener kafka:Listener cons = check new (kafka:DEFAULT_URL, consumerConfig);

service on cons {

    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
        // Process each record received from Kafka
        foreach kafka:AnydataConsumerRecord recordz in records {
            string value = recordz.value.toString();
            io:println("Received shipment request: ", value);
            
            // Deserialize and process the shipment
            sql_queries:Shipment shipment = check parseShipment(value);
            io:println("Processing shipment for express delivery...");
            
            // Serialize and send the response
            string serializedShipment = serializeShipment(shipment);
            kafka:Error? send = kafkaProducer->send({
                topic: "international-service",
                key: shipment.trackingNumber,
                value: serializedShipment.toBytes()
            });

            if send is kafka:Error {
                io:println("Error sending the processed shipment response: ", send.message());
            } else {
                io:println("Processed shipment response sent to international-service.");
            }
        }
    }
}

// Helper function to parse the shipment from a string (deserialize)
function parseShipment(string shipmentString) returns sql_queries:Shipment|error {
    // Parse the shipment string as a JSON object
    json shipmentData = check shipmentString.fromJsonString();

    // Extract the shipment details from the JSON object
    sql_queries:Shipment shipment = {
        customer_id: check 'int:fromString(check shipmentData.customer_id),
        shipment_id: check 'int:fromString(check shipmentData.shipment_id),
        shipmentType: check shipmentData.shipmentType,
        pickupLocation: check shipmentData.pickupLocation,
        deliveryLocation: check shipmentData.deliveryLocation,
        preferredTimeSlot: check shipmentData.preferredTimeSlot,
        trackingNumber: check shipmentData.trackingNumber
    };

    return shipment;
}

// Helper function to serialize the shipment to a string
function serializeShipment(sql_queries:Shipment shipment) returns string {
    return string `CustomerID: ${shipment.customer_id} | ShipmentID: ${shipment.shipment_id} | Type: ${shipment.shipmentType} | Pickup: ${shipment.pickupLocation} | DeliveryLocation: ${shipment.deliveryLocation} | TimeSlot: ${shipment.preferredTimeSlot} | TrackingNumber :${shipment.trackingNumber}`;
}

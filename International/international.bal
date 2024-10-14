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

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfig);

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfig);

listener kafka:Listener cons = new (kafka:DEFAULT_URL, {
groupId: "internationa_group",
topics: "international-service"
});


public function main() returns error? {
    io:println("International Delivery Service is running...");

while true {
    
    kafka:AnydataConsumerRecord[]|kafka:Error records = kafkaConsumer->poll(1000);

    if records is kafka:Error {
        io:println("Error polling records: ", records.message());
        continue; 
    }

    // Process each record
    foreach kafka:AnydataConsumerRecord recordz in records {
       
        string value = recordz.value.toString();

        io:println("Received shipment request: ", value);
        sql_queries:Shipment shipment = check parseShipment(value);

        io:println("Processing shipment for express delivery...");

        string serializedShipment = serializeShipment(shipment);
        kafka:Error? send = kafkaProducer->send({
            topic: "international-service",
            key: shipment.trackingNumber,
            value: serializedShipment.toBytes()
        });

        if send is kafka:Error {
            io:println("Error sending the processed shipment response");
        }
        io:println("Processed shipment response sent to processed_express_topic.");
    }
}

}

// Helper function to parse the shipment from a string (deserialize)
function parseShipment(string shipmentString) returns sql_queries:Shipment|error {
    // Parse the shipment string as a JSON object using fromJsonString
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
import ballerinax/kafka;
import ballerina/log;
import Logistics_S.sql_queries;

type ShipmentRequest record {
    string request_id;
    sql_queries:Shipment shipment_details;

};


// Kafka producer configuration
kafka:ProducerConfiguration producerConfig = {
    clientId: "logistics-producer",
    acks: "all",
    retryCount: 3
};

// Kafka consumer configuration
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "logistics-request-consumer",
    topics: ["logistics-service"]
};

// Initialize Kafka producer
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfig);

// Event-driven service using kafka:Listener
service on new kafka:Listener(kafka:DEFAULT_URL, consumerConfig) {

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord consumerRecord in records {
            byte[] messageContent = consumerRecord.value;
            string result = check string:fromBytes(messageContent);

            // Parse shipment request from JSON payload
            ShipmentRequest shipmentRequest = check result.fromJsonStringWithType();

            log:printInfo("Received request: " + shipmentRequest.toString());

            // Instead of using selectKafkaTopic, directly route based on shipment_type
            if shipmentRequest.shipment_details.shipment_type == "standard" {
                check kafkaProducer->send({
                    topic: "standard_delivery_requests",
                    key: shipmentRequest.shipment_details.trackingNumber,
                    value: serializeShipment(shipmentRequest.shipment_details)
                });
                log:printInfo("Routed to standard delivery");

            } else if shipmentRequest.shipment_details.shipment_type == "express" {
                check kafkaProducer->send({
                    topic: "express_delivery_requests",
                    key: shipmentRequest.shipment_details.trackingNumber,
                    value: serializeShipment(shipmentRequest.shipment_details)
                });
                log:printInfo("Routed to express delivery");

            } else if shipmentRequest.shipment_details.shipment_type == "international" {
                check kafkaProducer->send({
                    topic: "international_delivery_requests",
                    key: shipmentRequest.shipment_details.trackingNumber,
                    value: serializeShipment(shipmentRequest.shipment_details)
                });
                log:printInfo("Routed to international delivery");

            } else {
                // Default case if shipment_type is not recognized
                check kafkaProducer->send({
                    topic: "logistics_service",
                    key: shipmentRequest.shipment_details.trackingNumber,
                    value: serializeShipment(shipmentRequest.shipment_details)
                });
                log:printInfo("Routed to default logistics service");
            }
        }
    }
}


// Serialize the Shipment object into JSON
function serializeShipment(sql_queries:Shipment shipment) returns string {
    return shipment.toJsonString();
}


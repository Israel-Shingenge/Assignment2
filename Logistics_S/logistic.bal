import ballerinax/kafka;
import ballerina/io;
import Logistics_S.sql_queries;

kafka:ProducerConfiguration producerConfig = {
    clientId: "Logistic-producer",
    acks: "all",
    retryCount: 3
};

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "logistics-request-consumer",
    topics: ["logistics-service"],
    pollingInterval: 1
};

listener kafka:Listener cons = new (kafka:DEFAULT_URL, {
    groupId: "logistic_group",
    topics: "logistics-service"
});

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfig);

public function main() returns error? {
    io:println("Logistics Service is running...");

    while true {
        kafka:AnydataConsumerRecord[]|kafka:Error records = kafkaConsumer->poll(1000);
        
        if records is kafka:Error {
            io:println("Error polling records: ", records.message());
            continue;
        }

        foreach kafka:AnydataConsumerRecord recordz in records {
            sql_queries:Shipment shipment = createShipmentFromRecord(recordz);

            string topic = selectKafkaTopic(shipment.shipmentType);
            kafka:ProducerConfiguration producerConfig = {
                clientId: "logistics-producer",
                acks: "all",
                retryCount: 3
            };
            kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfig);

            check kafkaProducer->send({
                topic: topic,
                key: shipment.trackingNumber,
                value: serializeShipment(shipment) // Serialize the Shipment object before sending
            });
            io:println("Shipment request routed to: ", topic);
        }
    }
}

// Serialize the Shipment object into JSON
function serializeShipment(sql_queries:Shipment shipment) returns anydata {
    return shipment.toJson(); 
}

// Select the appropriate Kafka topic based on the shipment type
function selectKafkaTopic(string shipmentType) returns string {
    if shipmentType == "standard" {
        return "standard-service";
    } else if shipmentType == "express" {
        return "express-service";
    } else if shipmentType == "international" {
        return "international-service";
    } else {
        return "logistics-service";  // Default topic for other cases
    }
}

// Create a Shipment object from the Kafka record
function createShipmentFromRecord(kafka:AnydataConsumerRecord recordz) returns sql_queries:Shipment {
    string value = recordz.value.toString();
    return parseShipment(value); 
}

// Parse the shipment from the received string value
function parseShipment(string value) returns sql_queries:Shipment {
    // Implement the actual parsing logic based on the structure of the JSON payload
    return {
        customer_id: 0, 
        shipmentType: "", 
        pickupLocation: "", 
        deliveryLocation: "", 
        preferredTimeSlot: "", 
        trackingNumber: "", 
        shipment_id: 0
    };
}

import ballerina/io;
import ballerinax/kafka;
import ballerina/sql;
import ballerinax/java.jdbc;
import ballerina/random;
import ballerina/lang.runtime;

// Kafka consumer configuration
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "international-delivery-group",
    topics: ["international_delivery"],
    pollingInterval: 1,
    autoCommit: false
};

// Database configuration
jdbc:Client dbClient = check new ("jdbc:postgresql://localhost:5432/logistics_db", "postgres", "postgres");
// Initialize the Kafka consumer
kafka:Consumer kafkaConsumer = check new ("localhost:9092", consumerConfig);

public function main() {
    // Start consuming messages from Kafka
    kafka:AnydataConsumerRecord[] poll = check kafkaConsumer->poll(processMessage);
}

function processMessage(kafka:ConsumerRecord[] records) {
    foreach var record in records {
        json|error request = record.value.fromJsonString();
        if (request is error) {
            io:println("Error parsing JSON: ", request.message());
            return;
        }
        json requestJson = <json>request;

        // Extract shipmentId from the request
        int shipmentId = checkpanic requestJson.shipmentId.toInt();

        // Simulate processing the international delivery request
        // Generate a random customs clearance time (in seconds)
        int customsClearanceTime = checkpanic random:createIntInRange(10, 60);

        io:println(string `International delivery request for shipment ID: ${shipmentId} - Simulating customs clearance (takes ${customsClearanceTime} seconds)...`);
        runtime:sleep(<decimal>customsClearanceTime);

        // Update shipment status in the database
        sql:ParameterizedQuery query = `UPDATE shipments SET status = 'processed' WHERE shipment_id = ${shipmentId}`;
        sql:ExecutionResult result = checkpanic dbClient->execute(query);

        io:println(string `International delivery request processed for shipment ID: ${shipmentId}`);
    }
}

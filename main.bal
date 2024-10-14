import ballerina/io;
import ballerinax/kafka;
import ballerina/sql;
import ballerinax/java.jdbc;
import ballerina/random;

// Kafka consumer configuration
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "express-delivery-group",
    topics: ["express_delivery"],
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

        // Simulate processing the express delivery request
        // Generate a random delivery time between 1-3 days
        random:Generator generator = new;
        int deliveryDays = checkpanic generator.nextInt(1, 4); // Generates a random number between 1 (inclusive) and 4 (exclusive)
        string deliveryTime = string `${deliveryDays} days`;

        // Update shipment status and delivery time in the database
        sql:ParameterizedQuery query = `UPDATE shipments SET status = 'processed', preferred_time_slot = '${deliveryTime}' WHERE shipment_id = ${shipmentId}`;
        sql:ExecutionResult result = checkpanic dbClient->execute(query);

        io:println(string `Express delivery request processed for shipment ID: ${shipmentId}. Delivery time: ${deliveryTime}`);
    }
}

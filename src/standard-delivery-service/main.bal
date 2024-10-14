import ballerina/io;
import ballerinax/kafka;
import ballerina/sql;
import ballerinax/java.jdbc;
import ballerina/random;

// Kafka consumer configuration
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "standard-delivery-group",
    topics: ["standard_delivery"],
    pollingInterval: 1.0, // Decimal value
    autoCommit: false
};

// Database configuration
jdbc:Client dbClient = check new ("jdbc:postgresql://localhost:5432/logistics_db", "postgres", "postgres");

// Initialize the Kafka consumer
kafka:Consumer kafkaConsumer = check new ("localhost:9092", consumerConfig);

public function main() returns kafka:Error? {
    // Start consuming messages from Kafka
    check kafkaConsumer->subscribe(consumerConfig.topics);
    kafkaConsumer->ready();
    kafkaConsumer->fetch();
    kafka:AnyDataConsumerRecord[] poll = check kafkaConsumer->poll();
    processMessage(poll);
}

function processMessage(kafka:AnyDataConsumerRecord[] records) {
    foreach var record in records {
        json|error request = record.value.fromJsonString();
        if (request is error) {
            io:println("Error parsing JSON: ", request.message());
            return;
        }
        json requestJson = <json>request;

        // Extract shipmentId from the request
        int shipmentId = checkpanic requestJson.shipmentId.toInt();

        // Logic for Standard Delivery

        // Retrieve shipment details from the database
        sql:ParameterizedQuery shipmentQuery = `SELECT * FROM shipments WHERE shipment_id = ${shipmentId}`;
        stream<sql:Row, sql:Error?> shipmentStream = dbClient->query(shipmentQuery);
        sql:Row shipmentResult = checkpanic shipmentStream.next();
        if shipmentResult is sql:Error {
            io:println("Error retrieving shipment details: ", shipmentResult.message());
            return;
        }

        // Generate a random delivery time (simulating delivery process)
        int deliveryDays = checkpanic random:createIntInRange(3, 7); // Delivery in 3 to 7 days
        string deliveryTimeSlot = "Morning"; // You can add logic for different time slots

        // Update delivery schedule in the database
        sql:ParameterizedQuery scheduleQuery = `INSERT INTO delivery_schedules (shipment_id, pickup_time, delivery_time, tracking_information) VALUES (${shipmentId}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '${deliveryDays} days', 'Your package will be delivered in ${deliveryDays} days, ${deliveryTimeSlot}.') RETURNING schedule_id`;
        sql:ExecutionResult scheduleResult = checkpanic dbClient->execute(scheduleQuery);

        // Update shipment status in the database
        sql:ParameterizedQuery statusQuery = `UPDATE shipments SET status = 'scheduled' WHERE shipment_id = ${shipmentId}`;
        sql:ExecutionResult statusResult = checkpanic dbClient->execute(statusQuery);

        io:println(string `Standard delivery request processed for shipment ID: ${shipmentId}. Delivery scheduled in ${deliveryDays} days.`);
    }
}
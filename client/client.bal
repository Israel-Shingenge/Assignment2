import ballerina/io;
import ballerinax/kafka;

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new ("localhost:9092", producerConfiguration);

public function main() {
    // Get user input for the delivery request
    string shipmentType = io:readln("Enter shipment type (standard, express, international): ");
    string pickupLocation = io:readln("Enter pickup location: ");
    string deliveryLocation = io:readln("Enter delivery location: ");
    string preferredTimeSlot = io:readln("Enter preferred time slot: ");
    string firstName = io:readln("Enter your first name: ");
    string lastName = io:readln("Enter your last name: ");
    string contactNumber = io:readln("Enter your contact number: ");

    // Create the delivery request message
    string requestMessage = string `{"shipmentType": "<span class="math-inline">\{shipmentType\}", "pickupLocation"\: "</span>{pickupLocation}", "deliveryLocation": "<span class="math-inline">\{deliveryLocation\}", "preferredTimeSlot"\: "</span>{preferredTimeSlot}", "firstName": "<span class="math-inline">\{firstName\}", "lastName"\: "</span>{lastName}", "contactNumber": "${contactNumber}"}`;

    // Determine the appropriate Kafka topic based on the shipment type
    string topic;
    if (shipmentType == "standard") {
        topic = "standard_delivery";
    } else if (shipmentType == "express") {
        topic = "express_delivery";
    } else if (shipmentType == "international") {
        topic = "international_delivery";
    } else {
        io:println("Invalid shipment type.");
        return;
    }

    // Send the delivery request to the Kafka topic
    kafka:Error? sendResult = kafkaProducer->send({
        topic: topic,
        value: requestMessage
    });

    if (sendResult is error) {
        io:println("Error sending message to Kafka: ", sendResult);
    } else {
        io:println("Delivery request sent successfully.");
    }
}
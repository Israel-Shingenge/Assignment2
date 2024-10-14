import ballerina/io;
import ballerinax/kafka;

public function main() returns error? {
    kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

    json packageDetails = {
        "type": "standard",
        "pickupLocation": "Namibia",
        "deliveryLocation": "South Africa",
        "preferredTimeSlot": "morning",
        "customerInfo": {
            "firstName": "John",
            "lastName": "Doe",
            "contactNumber": "+264812345678"
        }
    };

    check kafkaProducer->send({
        topic: "package_requests",
        value: packageDetails.toString()
    });

    io:println("Package request sent successfully.");
    check kafkaProducer->flush();
}


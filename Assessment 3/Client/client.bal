import ballerinax/kafka;
import ballerina/io;
import logistics.sql_queries;

// Kafka producer configuration
kafka:ProducerConfiguration producerConfiguration = {
    clientId: "logistics-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfiguration);

public function main() {
    
    io:println("Welcome to the Central Logistics Service!");
    boolean repeat = true;

    while repeat {
        string hasAccount = io:readln("Do you have an account? (yes/no): ").trim().toLowerAscii();
        sql_queries:Customer customer;

        if hasAccount == "no" {
            customer = createAccount();
        } else if hasAccount == "yes" {
            customer = getCustomerInfo();
        } else {
            io:println("Invalid input. Please enter 'yes' or 'no'.");
            continue;
        }

        int|error result = sql_queries:addCustomer(customer);
        if result is error {
            io:println("Error inserting customer into the database: ", result);
        } else {
            io:println("Customer added successfully with ID: ", result);
        }

        sql_queries:Shipment shipment = getShipmentDetails(customer);
        int|error shipmentResult = sql_queries:addShipmentDetails(shipment);
        if shipmentResult is error {
            io:println("Error inserting shipment into the database: ", shipmentResult);
        } else {
            io:println("Shipment added successfully with ID: ", shipmentResult);
        }

        error? kafkaError = sendShipmentToTopic(shipment);
        if kafkaError is error {
            io:println("Error sending shipment request to Kafka topic: ", kafkaError);
        } else {
            io:println("Shipment request sent successfully to Kafka topic.");
        }

        io:println("Do you want to perform another operation? (Yes/No): ");
        string response = io:readln().trim().toUpperAscii();
        if response == "NO" {
            repeat = false;
        }
    }

    io:println("Exiting... Thank you for using the Central Logistics Service.");
}

// Send shipment details to a fixed Kafka topic for the logistic-service
function sendShipmentToTopic(sql_queries:Shipment shipment) returns error? {
    string topic = "logistics_service";  // Fixed topic for the logistics service

    json shipmentData = shipment.toJson(); // Serialize the Shipment object to JSON

    // Send the shipment data to the fixed logistics-service topic
    check kafkaProducer->send({
        topic: topic,
        value: shipmentData.toString()
    });

    io:println("Shipment request sent to topic: ", topic);
}



// Function to create a new customer account
function createAccount() returns sql_queries:Customer {
    io:println("Let's create your account.");
    io:print("Enter First Name: ");
    string firstName = io:readln();
    io:print("Enter Last Name: ");
    string lastName = io:readln();
    io:print("Enter Contact Number: ");
    string contactNumber = io:readln();

    sql_queries:Customer customer = {
        first_name: firstName,
        last_name: lastName,
        contact_number: contactNumber
    };

    io:println("Account created successfully!");
    return customer;
}

// Function to get customer information
function getCustomerInfo() returns sql_queries:Customer {
    io:println("Please provide your details.");
    io:print("Enter First Name: ");
    string firstName = io:readln();
    io:print("Enter Last Name: ");
    string lastName = io:readln();
    io:print("Enter Contact Number: ");
    string contactNumber = io:readln();

    sql_queries:Customer customer = {
        first_name: firstName,
        last_name: lastName,
        contact_number: contactNumber
    };

    return customer;
}

// Function to get shipment details from the user
function getShipmentDetails(sql_queries:Customer customer) returns sql_queries:Shipment {
    io:println("Please provide the shipment details.");
    io:print("Shipment Type (standard/express/international): ");
    string shipmentType = io:readln();
    io:print("Pickup Location: ");
    string pickupLocation = io:readln();
    io:print("Delivery Location: ");
    string deliveryLocation = io:readln();
    io:print("Preferred Time Slot (yyyy-MM-dd): ");
    string preferredTimeSlot = io:readln();

    sql_queries:Shipment shipmentRequest = {
        customer_id: <int>customer.customer_id,
        shipmentType: shipmentType,
        pickupLocation: pickupLocation,
        deliveryLocation: deliveryLocation,
        preferredTimeSlot: preferredTimeSlot,
        trackingNumber: "TRACK123",
        shipment_id: 0
    };
    return shipmentRequest;
}

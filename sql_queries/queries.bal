import ballerinax/mysql;
import ballerina/sql;

configurable string USER = "root";
configurable string PASSWORD = "israel";
configurable string HOST = "127.0.0.1";
configurable int PORT = 3306;
configurable string DATABASE = "company";

public type Customer record {|
    int customer_id?;
    string first_name;
    string last_name;
    string contact_number;
|};

public type Shipment record {| 
    int customer_id; 
    string shipmentType; 
    string pickupLocation; 
    string deliveryLocation; 
    string preferredTimeSlot; 
    string trackingNumber; 
|};


public type DeliveryService record {|
    int service_id;
    string serviceName;
|};

public type Schedule record  {|
    int shipmentId;
    string pickupTime;
    string deliveryTime;
|};

final mysql:Client dbClient = check new(
    host=HOST, user=USER, password=PASSWORD, port=PORT, database="company"
);

public isolated function addCustomer(Customer cus) returns int|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO Customer (customer_id, first_name, last_name, contact_number)
        VALUES (${cus.customer_id}, ${cus.first_name}, ${cus.last_name},  
                ${cus.contact_number})
    `);
    int|string? lastInsertId = result.lastInsertId;
    if lastInsertId is int {
        return lastInsertId;
    } else {
        return error("Unable to obtain last insert ID");
    }
}

isolated function updateCustomer(Customer cus) returns int|error {
    sql:ExecutionResult result = check dbClient->execute(`
        UPDATE Customer SET
            first_name = ${cus.first_name}, 
            last_name = ${cus.last_name},
            email = ${cus.contact_number},
        WHERE employee_id = ${cus.customer_id}  
    `);
    int|string? lastInsertId = result.lastInsertId;
    if lastInsertId is int {
        return lastInsertId;
    } else {
        return error("Unable to obtain last insert ID");
    }
}

public isolated function addShipmentDetails( Shipment ship) returns int|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO Shipment (customer_id, shipmentType, pickupLocation, deliveryLocation, preferedTimeSlot, trackingNumber)
        VALUES (${ship.customer_id}, ${ship.shipmentType}, ${ship.pickupLocation}, 
                ${ship.deliveryLocation}, ${ship.preferredTimeSlot}, ${ship.trackingNumber})
    `);
    int|string? lastInsertId = result.lastInsertId;
    if lastInsertId is int {
        return lastInsertId; 
    } else {
        return error("Unable to obtain last insert ID");
    }
}


public isolated function addDeliveryService( DeliveryService  dls) returns int|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO DeliveryService (service_id,serviceName)
        VALUES (${dls.service_id}, ${dls.serviceName})
    `);
    int|string? lastInsertId = result.lastInsertId;
    if lastInsertId is int {
        return lastInsertId;
    } else {
        return error("Unable to obtain last insert ID");
    }
}



public isolated function addSchedule (Schedule shed) returns int|error{
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO DeliveryService (shipmentId,pickupTime,deliveryTime)
        VALUES (${shed.shipmentId}, ${shed.pickupTime},${shed.deliveryTime})
    `);
    int|string? lastInsertId = result.lastInsertId;
    if lastInsertId is int {
        return lastInsertId;
    } else {
        return error("Unable to obtain last insert ID");
    }

}

public type Configuration record {|
    string host;
    int port;
    string database;
    string collection;
|};


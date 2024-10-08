FACULTY OF COMPUTING AND INFORMATICS
DEPARTMENT OF SOFTWARE ENGINEERING
Assignment 2
Course Title: Distributed Systems and Applications 
Course Code: DSA612S 
Assessment: Second Assignment
Released on: 25/09/2024. 
Due date: 14/10/ 2024 at 11h59AM
Total Marks: 100
Containerized Microservices with Ballerina and Kafka
Description
In this project, we are working on part of a larger logistics system. This system handles 
requests for package delivery from multiple providers, offering specialized services for 
different types of shipments, such as standard delivery, express delivery, and international 
delivery. The focus is on situations where customers need to schedule a package pickup 
and delivery service.
Here’s how it works:
1. When a customer wants to send a package, they submit a request to the central 
logistics service through Kafka as a middleware. This request includes details such 
as the type of shipment (standard, express, international), pickup location, delivery 
location, preferred time slots, and customer information (first name, last name, 
contact number).
2. The logistics service processes the request and communicates with various delivery 
services (standard, express, international) to find the best available time for the 
pickup and delivery based on the customer’s preferences. These delivery services 
may need to coordinate to determine the optimal route and schedule, especially for 
international deliveries.
3. Once the pickup and delivery times are confirmed, the logistics service sends the 
complete details back to the customer, including tracking information and estimated 
delivery time.
Your job is to create and set up this logistics sub-system.
Important things to note:
a) The system should be designed using a microservices architecture with a central 
logistics service and three specialized services: standard delivery, express 
delivery, and international delivery.
b) Use a Kafka instance to support the communication between the client and the 
various services.
c) Use a data store such as MongoDB or SQL to store customer information, 
shipment details, and delivery schedules.
d) Finally, deploy each service (logistics, standard, express, and international 
delivery) in separate Docker containers.
Evaluation Criteria:
● Setup of the Kafka cluster, including topic management. (30 marks)
● Setup of the MongoDB/SQL database instance. (10 marks)
● Docker container configuration. (10 marks)
● Implementation of Client and Services in Ballerina (50 marks)
Submission Instructions
• This assignment is to be completed by groups of minimum 5 and maxi of 7 students 
each.
• For each group, a repository should be created on Github. The repository should have 
all group members set up as contributors.
• All assignments must be completed from GitHub repository. Students who haven't 
pushed any codes to the repository will not be given the opportunity to present and 
defend the assignment. More particularly, if a student’s username does not appear 
in the commit log of the group repository, that student will be assumed not to have 
contributed to the project and thus be awarded the mark 0.
• The assignment will be group work, but individual marks will be allocated based on 
each student's contribution to the assignment.
• Marks for the assignment will only be allocated to students who have presented the 
assignment.
• It’s the responsibility of all group members to make sure that they are available for the 
assignment presentation. An assignment cannot be presented more than once. 
• The submission date is Monday, October 14, 2024, at 11h59AM. Please note that 
commits after that deadline will not be accepted. Therefore, a submission will be 
assessed based on the clone of the repository at the deadline.
• Any group that fails to submit on time will be awarded the mark 0. Late Submission will 
not be considered.
• There should be no assumption about the execution environment of your code. It could 
be run using a specific framework or on the command line.

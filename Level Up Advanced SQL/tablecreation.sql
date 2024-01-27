/* 
 * 
 * CREATE TABLE employees, customers, model, sales and inventory
 * 
 * 
*/

drop table employee;
drop table customer;
drop table model;
drop table inventory;
drop table sales;

create table employee (
    employeeID VARCHAR(100) primary key not null,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    title VARCHAR(100),
    startDate DATE,
    managerID VARCHAR(100)
);


create table customer (
    customerID SERIAL primary KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(100),
    zipcode VARCHAR(15),
    email VARCHAR(100)
);



create table model ( 
    modelID SERIAL primary key not null,
    model VARCHAR(150),
    EngineType VARCHAR(100)
);

--CREATE SEQUENCE model_id_seq START 1;
--
--CREATE TABLE model (
--    modelID INTEGER DEFAULT nextval('model_id_seq') PRIMARY KEY NOT NULL,
--    model VARCHAR(150),
--    EngineType VARCHAR(100)
--);
--
---- Set the sequence to start from 0
--SELECT setval('model_id_seq', 0);




create table inventory (
    inventoryID SERIAL primary key not null,
    modelID INT,
    color VARCHAR(10),
    year INT,
    isAvailable INT,
    foreign key(modelID) REFERENCES model(modelID)
);


create table sales ( 
    salesID SERIAL primary key not null,
    customerID INT,
    inventoryID INT,
    employeeID VARCHAR(100),
    salesAmount numeric(10, 2),
    soldDate DATE,
    foreign key (customerID) references customer(customerID),
    foreign key (inventoryID) REFERENCES inventory(inventoryID),
    foreign key (employeeID) references employee(employeeID)
);



CREATE TABLE EVENTS ( 
ID VARCHAR(256) PRIMARY KEY NOT NULL,
SUBSCRIPTION_ID VARCHAR(256), 
MESSAGE VARCHAR(2048) NOT NULL,
CREATED TIMESTAMP,
FOREIGN KEY (SUBSCRIPTION_ID) REFERENCES EVENTS(ID)
);
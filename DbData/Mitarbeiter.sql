CREATE TABLE sales.Mitarbeiter
    (
        ID           int PRIMARY KEY,
        Name         varchar(100),
        Manager_id   int,
        Abteilung  varchar(100)
    );

INSERT INTO sales.Mitarbeiter VALUES (1,  'Shripadh', NULL, 'CEO');
INSERT INTO sales.Mitarbeiter VALUES (2,  'Satya', 5, 'Software Engineer');
INSERT INTO sales.Mitarbeiter VALUES (3,  'Jia', 5, 'Data Analyst');
INSERT INTO sales.Mitarbeiter VALUES (4,  'David', 5, 'Data Scientist');
INSERT INTO sales.Mitarbeiter VALUES (5,  'Michael', 7, 'Manager');
INSERT INTO sales.Mitarbeiter VALUES (6,  'Arvind', 7, 'Architect');
INSERT INTO sales.Mitarbeiter VALUES (7,  'Asha', 1, 'CTO');
INSERT INTO sales.Mitarbeiter VALUES (8,  'Maryam', 1, 'Manager');
INSERT INTO sales.Mitarbeiter VALUES (9,  'Reshma', 8, 'Business Analyst');
INSERT INTO sales.Mitarbeiter VALUES (10, 'Akshay', 8, 'Java Developer');
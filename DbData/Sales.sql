CREATE schema sales;

CREATE TABLE sales.Verkaeufe
(
	Kontinent varchar(20),
	Land varchar(20),
	Stadt varchar(20),
	menge_Verkaeufe integer
);

INSERT INTO sales.Verkaeufe VALUES ('Nord Amerika', 'Kanada', 'Toronto', 10000);
INSERT INTO sales.Verkaeufe VALUES ('Nord Amerika', 'Kanada', 'Montreal', 5000);
INSERT INTO sales.Verkaeufe VALUES ('Nord Amerika', 'Kanada', 'Vancouver', 15000);
INSERT INTO sales.Verkaeufe VALUES ('Asien', 'China', 'Hong Kong', 7000);
INSERT INTO sales.Verkaeufe VALUES ('Asien', 'China', 'Peking', 5000);
INSERT INTO sales.Verkaeufe VALUES ('Asien', 'China', 'Shanghai', 3000);
INSERT INTO sales.Verkaeufe VALUES ('Europa', 'UK', 'London', 7000);
INSERT INTO sales.Verkaeufe VALUES ('Europa', 'UK', 'Manchester', 12000);
INSERT INTO sales.Verkaeufe VALUES ('Europa', 'Frankfreich', 'Paris', 12000);
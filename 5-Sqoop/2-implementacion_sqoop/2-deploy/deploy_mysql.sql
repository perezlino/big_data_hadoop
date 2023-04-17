--
-- @author ALONSO RAÚL MELGAREJO GALVÁN
-- 
-- @email alonsoraulmgs@gmail.com
-- @linkedin https://www.linkedin.com/in/alonsoraulmg
-- @facebook https://www.facebook.com/alonsoraulmg
-- @copyright Big Data Academy
-- 
-- Tablas para el esquema "main".
--

-- Conectarse a MySQL
-- mysql -uroot -pbigdataacademy

-- Creación del esquema
CREATE SCHEMA IF NOT EXISTS main;

-- Creación de tabla
CREATE TABLE IF NOT EXISTS main.TARJETA_TIPO(
ID INTEGER,
DESCRIPCION VARCHAR(128)
);

-- Creación de tabla
CREATE TABLE IF NOT EXISTS main.TARJETA_CLIENTE(
ID_PERSONA INTEGER,
ID_TIPO_TARJETA INTEGER
);

-- Datos
TRUNCATE TABLE main.TARJETA_TIPO;
INSERT INTO main.TARJETA_TIPO VALUES
(1, 'Basica'),
(2, 'Oro'),
(3, 'Black'),
(4, 'Platinum');

-- Datos
TRUNCATE TABLE main.TARJETA_CLIENTE;
INSERT INTO main.TARJETA_CLIENTE VALUES
(2,3), (26,4), (92,4), (15,2), (24,4), (58,3), (63,3), (80,2), (87,2), (21,3), (59,2), (46,3), (32,2), (30,3), (69,4), (75,1), (27,2), (4,2), (91,4), (59,1), (79,4), (29,2), (99,2), (32,1), (48,2), (53,3), (55,3), (58,4), (70,2), (33,2), (79,4), (1,2), (7,1), (45,3), (47,1), (11,4), (32,4), (79,3), (79,4), (46,4), (12,2), (5,4), (26,1), (90,1), (54,2), (42,1), (59,2), (77,2), (64,2), (45,4), (18,3), (68,2), (99,4), (75,4), (57,4), (9,3), (2,2), (98,2), (79,3), (22,4), (10,4), (49,1), (75,3), (66,4), (15,4), (22,2), (9,1), (30,1), (36,2), (27,3), (35,1), (61,2), (8,1), (90,2), (1,4), (47,4), (79,4), (17,2), (58,3), (41,3), (16,4), (1,3), (100,4), (4,1), (18,2), (68,4), (68,1), (2,1), (32,4), (53,4), (23,3), (2,3), (35,3), (43,1), (61,3), (97,1), (82,3), (64,1), (63,4), (45,3), (52,2), (45,4), (13,3), (38,3), (31,3), (96,3), (62,4), (13,4), (8,1), (78,1), (100,3), (26,2), (80,1), (15,4), (48,4), (54,3), (88,2), (85,2), (24,4), (88,4), (34,1), (46,4), (83,4), (35,4), (78,1), (25,3), (78,3), (11,1), (42,3), (25,4), (15,3), (49,2), (79,2), (85,4), (28,3), (85,1), (61,1), (75,4), (6,3), (49,1), (76,3), (96,4), (11,4), (47,3), (27,4), (33,1), (81,2), (36,2), (98,2), (95,1), (63,1), (45,2), (33,3), (48,3), (22,2), (2,2), (38,2), (60,4), (75,4), (52,1);

exit;
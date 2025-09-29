DROP TABLE IF EXISTS modu_tec;
DROP TABLE IF EXISTS tecnologia;
DROP TABLE IF EXISTS modulo;
DROP TABLE IF EXISTS profesor;
DROP TABLE IF EXISTS matricula;
DROP TABLE IF EXISTS bootcamp;
DROP TABLE IF EXISTS alumno;

CREATE TABLE alumno (
    alumno_id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    apellido VARCHAR(255) NOT NULL,
    identificador_fiscal VARCHAR(20) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE bootcamp (
    bootcamp_id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL
);

CREATE TABLE matricula (
    matricula_id SERIAL PRIMARY KEY,
    bootcamp_id INT NOT NULL,
    alumno_id INT NOT NULL,
    fecha_matriculacion DATE NOT NULL,
    FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id),
    FOREIGN KEY (alumno_id) REFERENCES alumno(alumno_id),
    UNIQUE (bootcamp_id, alumno_id, fecha_matriculacion)
);

CREATE TABLE profesor (
    profesor_id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    apellido VARCHAR(255) NOT NULL,
    identificador_fiscal VARCHAR(20) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE modulo (
    modulo_id SERIAL PRIMARY KEY,
    profesor_id INT,
    bootcamp_id INT,
    nombre VARCHAR(255) NOT NULL,
    horas INT NOT NULL,
    FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id),
    FOREIGN KEY (profesor_id) REFERENCES profesor(profesor_id)
);

CREATE TABLE tecnologia (
    tecnologia_id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    fecha_lanzamiento DATE NOT NULL
);

CREATE TABLE modu_tec (
    modu_tec_id SERIAL PRIMARY KEY,
    modulo_id INT,
    tecnologia_id INT,
    FOREIGN KEY (modulo_id) REFERENCES modulo(modulo_id),
    FOREIGN KEY (tecnologia_id) REFERENCES tecnologia(tecnologia_id),
    UNIQUE (modulo_id, tecnologia_id)
);

-- Insertar registros en la tabla alumno
INSERT INTO alumno (nombre, apellido, identificador_fiscal, email) VALUES
    ('Juan', 'Perez', '123456789', 'juan.perez@email.com'),
    ('Maria', 'Gomez', '987654321', 'maria.gomez@email.com'),
    ('Carlos', 'Rodriguez', '456789012', 'carlos.rodriguez@email.com'),
    ('Laura', 'Lopez', '789012345', 'laura.lopez@email.com'),
    ('Pedro', 'Martinez', '234567890', 'pedro.martinez@email.com');

-- Insertar registros en la tabla bootcamp
INSERT INTO bootcamp (nombre) VALUES
    ('Bootcamp Web'),
    ('Bootcamp Mobile'),
    ('Bootcamp IA'),
    ('Bootcamp Big Data'),
    ('Bootcamp Ciberseguridad');

-- Insertar registros en la tabla matricula
INSERT INTO matricula (bootcamp_id, alumno_id, fecha_matriculacion) VALUES
    (1, 1, '2023-01-01'),
    (2, 2, '2023-02-01'),
    (3, 3, '2023-03-01'),
    (4, 4, '2023-04-01'),
    (5, 5, '2023-05-01');

-- Insertar registros en la tabla profesor
INSERT INTO profesor (nombre, apellido, identificador_fiscal, email) VALUES
    ('Ana', 'Garcia', '543210987', 'ana.garcia@email.com'),
    ('Mario', 'Diaz', '876543210', 'mario.diaz@email.com'),
    ('Elena', 'Fernandez', '345678901', 'elena.fernandez@email.com'),
    ('David', 'Ruiz', '210987654', 'david.ruiz@email.com'),
    ('Isabel', 'Sanchez', '678901234', 'isabel.sanchez@email.com');

-- Insertar registros en la tabla modulo
INSERT INTO modulo (profesor_id, bootcamp_id, nombre, horas) VALUES
    (1, 1, 'HTML+CSS', 20),
    (2, 2, 'IOS+Swift', 15),
    (3, 3, 'SQL Avanzado', 25),
    (4, 4, 'NLP', 30),
    (5, 5, 'Criptografía', 18);

-- Insertar registros en la tabla tecnologia
INSERT INTO tecnologia (nombre, fecha_lanzamiento) VALUES
    ('Docker', '2015-01-23'),
    ('Python', '1991-02-20'),
    ('JavaScript', '1995-12-04'),
    ('Swift', '2018-10-01'),
    ('SQL', '1974-01-01');

-- Insertar registros en la tabla modu_tec
INSERT INTO modu_tec (modulo_id, tecnologia_id) VALUES
    (1, 1),
    (2, 4),
    (3, 5),
    (4, 2),
    (5, 3);

SELECT alumno.email AS email_alumno
	 , matricula.matricula_id
	 , matricula.fecha_matriculacion
	 , bootcamp.nombre AS bootcamp
	 , modulo.nombre AS modulo
	 , profesor.email AS email_profesor
	 , tecnologia.nombre AS tecnologia
  FROM alumno
  JOIN matricula
    ON alumno.alumno_id = matricula.alumno_id
  JOIN bootcamp
    ON matricula.bootcamp_id = bootcamp.bootcamp_id
  JOIN modulo
    ON bootcamp.bootcamp_id = modulo.bootcamp_id
  JOIN profesor
    ON modulo.profesor_id = profesor.profesor_id
  JOIN modu_tec
    ON modulo.modulo_id = modu_tec.modulo_id
  JOIN tecnologia
    ON modu_tec.tecnologia_id = tecnologia.tecnologia_id;
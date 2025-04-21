# BD.py
import sqlite3

def base_datos(ruta: str = 'Hospital.db'):
    conn = sqlite3.connect(ruta)
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys = ON;')

    cursor.executescript('''

    -- Tabla de los Pacientes
    CREATE TABLE IF NOT EXISTS Pacientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        edad INTEGER NOT NULL,
        sexo TEXT CHECK(sexo IN ('M', 'F')) NOT NULL,
        estado_inicial INTEGER CHECK(estado_inicial IN (0, 1, 2)), -- 0:grave, 1:delicado, 2:normal
        llegada DATETIME NOT NULL,
        diagnosticado BOOLEAN DEFAULT 0,
        cama INTEGER,
        medico INTEGER,
        estado_final TEXT CHECK(estado_final IN ('Fallecido', 'Alta')),
        FOREIGN KEY(cama) REFERENCES Camas(id),
        FOREIGN KEY(medico) REFERENCES Medicos(id)
    );

    -- Tabla de los Medicamentos
    CREATE TABLE IF NOT EXISTS Medicamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre VARCHAR(50) UNIQUE NOT NULL,
        cantidad INTEGER DEFAULT 0
    );

    -- Tabla de las Camas
    CREATE TABLE IF NOT EXISTS Camas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        estado TEXT CHECK(estado IN ('Disponible', 'Ocupado')) NOT NULL DEFAULT 'Disponible'
    );

    -- Tabla de las Prescripciones
    CREATE TABLE IF NOT EXISTS Prescripciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        paciente_id INTEGER NOT NULL,
        medicamento_id INTEGER NOT NULL,
        cantidad INTEGER NOT NULL,
        fecha DATETIME NOT NULL, -- Añadir fecha de prescripción puede ser útil
        FOREIGN KEY(paciente_id) REFERENCES Pacientes(id),
        FOREIGN KEY(medicamento_id) REFERENCES Medicamentos(id)
    );

    -- Tabla de los Medicos
    CREATE TABLE IF NOT EXISTS Medicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pacientes_asignados INTEGER DEFAULT 0
    );

    -- Tabla de Historial_Etapas
    CREATE TABLE IF NOT EXISTS Historial_Etapas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        paciente_id INTEGER NOT NULL,
        -- **** CORREGIR AQUÍ ****
        etapa TEXT CHECK(etapa IN ('Registro', 'Diagnóstico', 'Asignación', 'Seguimiento')) NOT NULL,
        fecha DATETIME NOT NULL,
        prioridad INTEGER CHECK(prioridad IN (0, 1, 2)) NOT NULL,
        FOREIGN KEY(paciente_id) REFERENCES Pacientes(id)
    );

    -- -----------------------------------------
    -- INSERCIONES INICIALES (SI LAS TABLAS ESTÁN VACÍAS)
    -- -----------------------------------------

    -- Insertar medicamentos iniciales (ejemplo)
    -- Usamos INSERT OR IGNORE para evitar errores si ya existen (por nombre UNIQUE)
    INSERT OR IGNORE INTO Medicamentos (nombre, cantidad) VALUES
        ('Paracetamol 500mg', 1000),
        ('Ibuprofeno 400mg', 800),
        ('Amoxicilina 250mg', 500),
        ('Omeprazol 20mg', 600),
        ('Salbutamol Inhalador', 300);

    ''') # Fin de executescript

    # Puedes añadir aquí también la lógica para sembrar Camas y Médicos
    # si quieres que esté todo centralizado en BD.py, aunque dejarlo
    # en main() como está ahora también es válido.

    # Commit final para guardar todos los cambios de executescript
    conn.commit()
    print(f"Base de datos '{ruta}' inicializada/actualizada.")
    return conn
# Proyectos de PPC: Simulación Hospitalaria y Análisis de Movilidad Urbana

Este repositorio contiene dos proyectos principales:

1.  **Simulación de Flujo Hospitalario:** Una simulación del flujo de pacientes en urgencias usando concurrencia y paralelismo en Python.

2.  **Análisis de Movilidad Urbana (Metro CDMX):** Un análisis de la afluencia del Metro de la Ciudad de México utilizando PySpark.

---

## 1. Simulación de Flujo Hospitalario con Concurrencia y Paralelismo

Este proyecto simula el flujo de pacientes a través de las diferentes etapas de atención en la sala de urgencias de un hospital automatizado. Utiliza técnicas de programación concurrente, paralela y asíncrona en Python para modelar la llegada, registro, diagnóstico, asignación de recursos y alta de pacientes con diferentes niveles de prioridad.


### Descripción General

La simulación modela las siguientes etapas:

1.  **Llegada de Pacientes:** Los pacientes llegan al hospital siguiendo una distribución de probabilidad exponencial.
2.  **Registro:** Se registran los datos básicos del paciente en la base de datos.
3.  **Diagnóstico:** Se simula un diagnóstico, incluyendo una fase intensiva en CPU (ejecutada en paralelo) y latencias simuladas (ej. consulta a IA/API externa). Los pacientes se procesan según su prioridad (Grave > Delicado > Normal).
4.  **Asignación de Recursos:** Se asignan camas (si son necesarias) y médicos disponibles, gestionando la disponibilidad y concurrencia.
5.  **Seguimiento y Alta:** Se simula el tiempo de recuperación del paciente y se liberan los recursos asignados antes de darlo de alta.

### Características Técnicas Principales

* **Modelo Asíncrono:** Uso intensivo de `asyncio` para manejar operaciones concurrentes y esperas (I/O simulado, sleeps, colas).
* **Procesamiento Paralelo:** Uso de `concurrent.futures.ProcessPoolExecutor` para simular tareas de diagnóstico que requieren uso intensivo de CPU.
* **Gestión de Prioridades:** Implementación de `asyncio.PriorityQueue` para manejar pacientes según su estado de gravedad y orden de llegada.
* **Base de Datos:**
    * **Primaria:** SQLite (`Hospital_Simulacion.db`) para el estado en tiempo real de la simulación (pacientes, camas, médicos, historial).
* **Gestión de Recursos:** Lógica para asignar y liberar recursos limitados (camas, médicos) consultando y actualizando la base de datos SQLite, manejando la concurrencia.

### Tecnologías Utilizadas (Simulación Hospitalaria)

* Python 3.10+
* `asyncio`
* `concurrent.futures`
* `sqlite3`
* `numpy` (para la generación de llegadas)


---

## 2. Análisis de Movilidad Urbana con PySpark: Metro CDMX

Este proyecto aplica Apache Spark (PySpark) para analizar datos de afluencia del Metro de la Ciudad de México, buscando identificar patrones de uso y estacionalidad.

*(Puedes encontrar este proyecto en la carpeta: `[ruta/al/analisis_metro]`)*

### Dataset

* **Fuente:** Datos Abiertos CDMX.
* **Contenido:** Registros de afluencia diaria por estación.

### Objetivos Generales

1.  Identificar las líneas y estaciones con mayor volumen de pasajeros.
2.  Determinar patrones generales de afluencia mensual y semanal.


### Tecnologías Utilizadas (Análisis Metro)

* Python 3, PySpark, Spark SQL
* Pandas, Matplotlib, Seaborn

---

# Librerías para concurrencia, asincronía, simulación y base de datos
import asyncio              # Para operaciones asíncronas (corutinas, event loop).
import random               # Para generar aleatoriedad (edades, estados, tiempos).
import numpy as np          # Para cálculos numéricos (distribución exponencial de llegadas).
import datetime             # Para manejar fechas y horas (timestamps de eventos).
from dataclasses import dataclass, field # Para crear clases de datos fácilmente (Paciente).
import time                 # Para funciones de tiempo (simular duraciones con sleep).
import concurrent.futures   # Para ejecutar tareas bloqueantes (CPU) en paralelo (procesos).
import sqlite3              # Para interactuar con la base de datos SQLite (persistir estado).
import traceback            # Para obtener detalles de errores inesperados.
import os                   # Para interactuar con el sistema operativo (rutas de archivos, CPU count).



# -------------------------------------------
# Inicializa la conexión a la base de datos
# -------------------------------------------
def base_datos_wrapper(db_path: str = 'Hospital.db') -> sqlite3.Connection:
    """
    Abre o crea la base de datos usando la función base_datos de BD.py.
    """
    try:
        from BD import base_datos
        print(f"Intentando conectar/crear BD en: {os.path.abspath(db_path)}")
        conn = base_datos(db_path)
        print(f"Conexión a BD establecida: {conn}")
        return conn
        
    except ImportError:
        print("ERROR: No se pudo importar 'base_datos' desde BD.py.")
        raise

    except sqlite3.Error as e:
        print(f"ERROR al conectar o inicializar la BD '{db_path}': {e}")
        raise



# -------------------------------------------
# Clase que representa a un paciente
# -------------------------------------------
@dataclass(order=True)
class Paciente:
    """
    Representa un paciente en la simulación. Ordenable por prioridad y llegada.
    """

    prioridad: int = field(compare=True)             # Prioridad numérica (menor es más urgente). Usado para ordenar.
    timestamp_llegada: float = field(compare=True)   # Momento exacto de llegada (Unix timestamp). Usado para ordenar.
    edad: int = field(compare=False)                 # Edad del paciente.
    sexo: str = field(compare=False)                 # Sexo ('M' o 'F').
    estado_actual: str = field(compare=False)        # Estado médico ('normal', 'delicado', 'grave').
    llegada: datetime.datetime = field(compare=False)# Hora de llegada (objeto datetime).
    id: int | None = field(default=None, compare=False) # ID único asignado por la BD (o None).
    diagnosticado: bool = field(default=False, compare=False) # Indica si ya fue diagnosticado.
    cama: int | None = field(default=None, compare=False)    # ID de la cama asignada (o None).
    medico: int | None = field(default=None, compare=False)  # ID del médico asignado (o None).
    estado_final: str | None = field(default=None, compare=False) # Resultado final ('Alta', 'Fallecido', o None).



# -------------------------------------------
# Simula un diagnóstico que consume CPU
# -------------------------------------------
def cpu_diagnostico(paciente_id: int, estado: str) -> bool:
    """
    Simula el diagnóstico de un paciente (tarea pesada).
    """
    sleep_time = random.uniform(0.5, 2.0)
    time.sleep(sleep_time)

    return True



# -------------------------------------------
# Genera pacientes y los pone en la cola de llegadas
# -------------------------------------------
async def generador_llegadas(lambda_rate: float, tiempo_simulacion: float, cola_llegadas: asyncio.Queue):
    """
    Genera pacientes de manera aleatoria durante el tiempo de simulación.
    """
    paciente_temp_id_counter = 1
    probabilidades = [0.7, 0.2, 0.1] # normal, delicado, grave
    estados = ['normal', 'delicado', 'grave']
    m_prio = {'grave': 0, 'delicado': 1, 'normal': 2}

    sim_start_time = asyncio.get_event_loop().time()
    print(f"[{datetime.datetime.now().isoformat()}] Iniciando generación de llegadas...")

    while True:
        real_current_time = asyncio.get_event_loop().time()
        simulated_elapsed_time = real_current_time - sim_start_time

        if simulated_elapsed_time >= tiempo_simulacion:
            print(f"[{datetime.datetime.now().isoformat()}] Tiempo de simulación ({tiempo_simulacion:.1f}s) alcanzado. Deteniendo llegadas.")
            break

        # Espera un tiempo aleatorio antes de crear el siguiente paciente
        delta_t = np.random.exponential(1.0 / lambda_rate)
        await asyncio.sleep(delta_t)

        # Crea un nuevo paciente
        estado = random.choices(estados, weights=probabilidades, k=1)[0]
        llegada_dt = datetime.datetime.now()
        llegada_ts = llegada_dt.timestamp()
        prioridad_num = m_prio[estado]

        paciente = Paciente(
            prioridad=prioridad_num,
            timestamp_llegada=llegada_ts,
            id=None,
            edad=random.randint(0, 99),
            sexo=random.choice(['M', 'F']),
            estado_actual=estado,
            llegada=llegada_dt,
        )
        print(f"[{llegada_dt.isoformat()}] Llega paciente (TempID:{paciente_temp_id_counter}, Prio:{prioridad_num}, Estado:{estado})")
        await cola_llegadas.put(paciente)
        paciente_temp_id_counter += 1

    # Señal de fin de llegadas
    await cola_llegadas.put(None)
    print(f"[{datetime.datetime.now().isoformat()}] Señal de fin de llegadas enviada.")

# -------------------------------------------
# Registra pacientes en la base de datos y los pasa a la siguiente etapa
# -------------------------------------------
async def consumidor_registro(cola_llegadas: asyncio.Queue, cola_prioridad: asyncio.PriorityQueue, conn: sqlite3.Connection):
    """
    Toma pacientes de la cola de llegadas, los registra en la base de datos y los pasa a la cola de prioridad.
    """
    cursor = conn.cursor()
    m_db = {'normal': 0, 'delicado': 1, 'grave': 2}

    while True:
        paciente = await cola_llegadas.get()
        if paciente is None:
            print(f"[{datetime.datetime.now().isoformat()}] Registro: Recibida señal de fin. Propagando a Diagnóstico...")
            await cola_prioridad.put(None)
            cola_llegadas.task_done()
            break

        hora_registro = datetime.datetime.now()
        print(f"[{hora_registro.isoformat()}] Registrando paciente (Estado: {paciente.estado_actual}, Prio:{paciente.prioridad})...")
        try:
            # Guarda el paciente en la base de datos
            cursor.execute(
                """
                INSERT INTO Pacientes
                  (edad, sexo, estado_inicial, llegada, diagnosticado, cama, medico, estado_final)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (paciente.edad, paciente.sexo, paciente.prioridad,
                 paciente.llegada.isoformat(), False, None, None, None)
            )
            paciente.id = cursor.lastrowid

            # Guarda el historial de registro
            cursor.execute(
                """
                INSERT INTO Historial_Etapas (paciente_id, etapa, fecha, prioridad)
                VALUES (?, ?, ?, ?)
                """,
                (paciente.id, 'Registro', hora_registro.isoformat(), paciente.prioridad)
            )
            conn.commit()
            print(f"[{hora_registro.isoformat()}] Paciente {paciente.id} registrado. Enviando a diagnóstico.")

            # Pasa el paciente a la siguiente etapa
            await cola_prioridad.put(paciente)

        except sqlite3.Error as e:
            print(f"ERROR en registro BD para paciente {getattr(paciente, 'id', 'PRE-REGISTRO')}: {e}")
        except Exception as e:
             print(f"ERROR inesperado en registro para paciente {getattr(paciente, 'id', 'PRE-REGISTRO')}: {e}")
        finally:
             cola_llegadas.task_done()

# -------------------------------------------
# Diagnostica pacientes usando procesos en paralelo
# -------------------------------------------
async def consumidor_diagnostico(cola_prioridad: asyncio.PriorityQueue, cola_asignacion: asyncio.Queue, conn: sqlite3.Connection, executor: concurrent.futures.Executor):
    """
    Toma pacientes de la cola de prioridad, realiza diagnóstico (en paralelo) y los pasa a la cola de asignación.
    """
    cursor = conn.cursor()

    while True:
        paciente = await cola_prioridad.get()
        if paciente is None:
            print(f"[{datetime.datetime.now().isoformat()}] Diagnóstico: Recibida señal de fin. Propagando a Asignación...")
            await cola_asignacion.put(None)
            cola_prioridad.task_done()
            break

        if not isinstance(paciente, Paciente):
             print(f"WARN: Diagnóstico recibió algo inesperado: {paciente}. Ignorando.")
             cola_prioridad.task_done()
             continue

        hora_inicio_diag = datetime.datetime.now()
        print(f"[{hora_inicio_diag.isoformat()}] Diagnosticando paciente {paciente.id} (Prio: {paciente.prioridad})...")

        try:
            # Ejecuta el diagnóstico en otro proceso
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, cpu_diagnostico, paciente.id, paciente.estado_actual)

            # Simula una pequeña espera extra
            api_latency = random.uniform(0.1, 0.5)
            await asyncio.sleep(api_latency)
            hora_fin_diag = datetime.datetime.now()

            # Marca al paciente como diagnosticado en la base de datos
            cursor.execute(
                "UPDATE Pacientes SET diagnosticado = 1 WHERE id = ?", (paciente.id,)
            )
            paciente.diagnosticado = True

            # Guarda el historial de diagnóstico
            cursor.execute(
                """
                INSERT INTO Historial_Etapas (paciente_id, etapa, fecha, prioridad)
                VALUES (?, ?, ?, ?)
                """,
                (paciente.id, 'Diagnóstico', hora_fin_diag.isoformat(), paciente.prioridad)
            )
            conn.commit()
            print(f"[{hora_fin_diag.isoformat()}] Paciente {paciente.id} diagnosticado. Enviando a asignación.")

            # Pasa el paciente a la siguiente etapa
            await cola_asignacion.put(paciente)

        except sqlite3.Error as e:
            print(f"ERROR en diagnóstico BD para paciente {paciente.id}: {e}")
        except Exception as e:
             print(f"ERROR en proceso diagnóstico para paciente {paciente.id}: {e}")
        finally:
            cola_prioridad.task_done()

# -------------------------------------------
# Asigna cama y médico a los pacientes
# -------------------------------------------
async def consumidor_asignacion(cola_asignacion: asyncio.Queue, cola_seguimiento: asyncio.Queue, conn: sqlite3.Connection, sem_camas: asyncio.Semaphore, sem_medicos: asyncio.Semaphore):
    """
    Asigna recursos (cama y médico) a los pacientes y los pasa a la etapa de seguimiento.
    """
    cursor = conn.cursor()

    while True:
        paciente = await cola_asignacion.get()
        if paciente is None:
            print(f"[{datetime.datetime.now().isoformat()}] Asignación: Recibida señal de fin. Propagando a Seguimiento...")
            await cola_seguimiento.put(None)
            cola_asignacion.task_done()
            break

        if not isinstance(paciente, Paciente):
             print(f"WARN: Asignación recibió algo inesperado: {paciente}. Ignorando.")
             cola_asignacion.task_done()
             continue

        hora_inicio_asig = datetime.datetime.now()
        print(f"[{hora_inicio_asig.isoformat()}] Asignando recursos para paciente {paciente.id} (Estado: {paciente.estado_actual})...")

        cama_id = None
        medico_id = None
        cama_semaphore_acquired = False
        medico_semaphore_acquired = False

        try:
            # Si el paciente es delicado o grave, necesita cama
            if paciente.estado_actual in ['delicado', 'grave']:
                print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} necesita cama. Esperando semáforo...")
                await sem_camas.acquire()
                cama_semaphore_acquired = True
                print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} adquirió semáforo cama. Buscando cama libre...")
                cursor.execute("SELECT id FROM Camas WHERE estado='Disponible' LIMIT 1")
                row = cursor.fetchone()
                if row:
                    cama_id = row[0]
                    cursor.execute("UPDATE Camas SET estado='Ocupado' WHERE id=?", (cama_id,))
                    print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} asignado TENTATIVAMENTE a cama {cama_id}.")
                else:
                    print(f"WARN: Paciente {paciente.id} necesita cama pero NO HAY DISPONIBLES! Liberando semáforo.")
                    sem_camas.release()
                    cama_semaphore_acquired = False
                    cola_asignacion.task_done()
                    continue

            # Todos los pacientes necesitan médico
            print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} necesita médico. Esperando semáforo...")
            await sem_medicos.acquire()
            medico_semaphore_acquired = True
            print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} adquirió semáforo médico. Buscando médico libre...")
            cursor.execute(
                "SELECT id FROM Medicos WHERE pacientes_asignados < 5 ORDER BY pacientes_asignados ASC LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                medico_id = row[0]
                cursor.execute(
                    "UPDATE Medicos SET pacientes_asignados = pacientes_asignados + 1 WHERE id=?", (medico_id,)
                )
                print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} asignado TENTATIVAMENTE a médico {medico_id}.")
            else:
                print(f"WARN: Paciente {paciente.id} necesita médico pero NO HAY DISPONIBLES! Liberando semáforos...")
                sem_medicos.release()
                medico_semaphore_acquired = False
                if cama_semaphore_acquired:
                    print(f"WARN: Liberando cama {cama_id} de paciente {paciente.id} por falta de médico.")
                    cursor.execute("UPDATE Camas SET estado='Disponible' WHERE id=?", (cama_id,))
                    sem_camas.release()
                    cama_semaphore_acquired = False
                    cama_id = None
                cola_asignacion.task_done()
                continue

            # Si se asignaron todos los recursos, actualiza la base de datos
            hora_fin_asig = datetime.datetime.now()
            cursor.execute(
                "UPDATE Pacientes SET cama=?, medico=? WHERE id=?",
                (cama_id, medico_id, paciente.id)
            )
            paciente.cama = cama_id
            paciente.medico = medico_id

            cursor.execute(
                """
                INSERT INTO Historial_Etapas (paciente_id, etapa, fecha, prioridad)
                VALUES (?, ?, ?, ?)
                """,
                (paciente.id, 'Asignación', hora_fin_asig.isoformat(), paciente.prioridad)
            )
            conn.commit()
            print(f"[{hora_fin_asig.isoformat()}] Paciente {paciente.id} asignado a recursos (Cama:{cama_id}, Médico:{medico_id}). Enviando a seguimiento.")
            await cola_seguimiento.put(paciente)

        except sqlite3.Error as e:
            conn.rollback()
            print(f"ERROR en asignación BD para paciente {paciente.id}: {e}. Rollback realizado.")
            if cama_semaphore_acquired: sem_camas.release()
            if medico_semaphore_acquired: sem_medicos.release()
        except asyncio.CancelledError:
             print(f"Asignación para paciente {paciente.id} cancelada.")
             if cama_semaphore_acquired: sem_camas.release()
             if medico_semaphore_acquired: sem_medicos.release()
             raise
        except Exception as e:
            conn.rollback()
            print(f"ERROR inesperado en asignación para paciente {paciente.id}: {e}")
            if cama_semaphore_acquired: sem_camas.release()
            if medico_semaphore_acquired: sem_medicos.release()
        finally:
             if not asyncio.current_task().cancelled():
                cola_asignacion.task_done()

# -------------------------------------------
# Simula la recuperación y da de alta al paciente
# -------------------------------------------
async def consumidor_seguimiento(cola_seguimiento: asyncio.Queue, conn: sqlite3.Connection, sem_camas: asyncio.Semaphore, sem_medicos: asyncio.Semaphore):
    """
    Simula el tiempo de recuperación del paciente, libera recursos y da de alta.
    """
    cursor = conn.cursor()

    while True:
        paciente = await cola_seguimiento.get()
        if paciente is None:
            print(f"[{datetime.datetime.now().isoformat()}] Seguimiento: Recibida señal de fin. Terminando.")
            cola_seguimiento.task_done()
            break

        if not isinstance(paciente, Paciente):
             print(f"WARN: Seguimiento recibió algo inesperado: {paciente}. Ignorando.")
             cola_seguimiento.task_done()
             continue

        hora_inicio_seg = datetime.datetime.now()
        print(f"[{hora_inicio_seg.isoformat()}] Iniciando seguimiento para paciente {paciente.id} (Estado: {paciente.estado_actual})...")

        try:
            # Simula el tiempo de recuperación según el estado
            if paciente.estado_actual == 'normal':
                dur = random.uniform(1, 10)
            elif paciente.estado_actual == 'delicado':
                dur = random.uniform(11, 59)
            else:
                dur = random.uniform(60, 120)
            print(f"[{datetime.datetime.now().isoformat()}] Paciente {paciente.id} en recuperación por {dur:.2f} segundos...")
            await asyncio.sleep(dur)
            hora_fin_seg = datetime.datetime.now()

            # Libera recursos y actualiza la base de datos
            recursos_liberados = []
            if paciente.cama is not None:
                cursor.execute("UPDATE Camas SET estado='Disponible' WHERE id=?", (paciente.cama,))
                sem_camas.release()
                recursos_liberados.append(f"Cama {paciente.cama}")
                paciente.cama = None

            if paciente.medico is not None:
                cursor.execute("UPDATE Medicos SET pacientes_asignados = pacientes_asignados - 1 WHERE id=? AND pacientes_asignados > 0", (paciente.medico,))
                if cursor.rowcount > 0:
                     sem_medicos.release()
                     recursos_liberados.append(f"Médico {paciente.medico}")
                else:
                     print(f"WARN: Intento de decrementar contador para médico {paciente.medico} (paciente {paciente.id}) falló o ya era 0.")
                paciente.medico = None

            estado_final = 'Alta'
            cursor.execute(
                "UPDATE Pacientes SET estado_final = ? WHERE id = ?",
                (estado_final, paciente.id)
            )
            paciente.estado_final = estado_final

            cursor.execute(
                """
                INSERT INTO Historial_Etapas (paciente_id, etapa, fecha, prioridad)
                VALUES (?, ?, ?, ?)
                """,
                (paciente.id, 'Seguimiento', hora_fin_seg.isoformat(), paciente.prioridad)
            )
            conn.commit()
            print(f"[{hora_fin_seg.isoformat()}] Paciente {paciente.id} dado de ALTA. Recursos liberados: {', '.join(recursos_liberados) if recursos_liberados else 'Ninguno'}.")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"ERROR en seguimiento BD para paciente {paciente.id}: {e}. Rollback realizado.")
        except asyncio.CancelledError:
             print(f"Seguimiento para paciente {paciente.id} cancelado durante sleep/operación.")
             raise
        except Exception as e:
            conn.rollback()
            print(f"ERROR inesperado en seguimiento para paciente {paciente.id}: {e}")
        finally:
             if not asyncio.current_task().cancelled():
                cola_seguimiento.task_done()

# -------------------------------------------
# Función principal que orquesta toda la simulación
# -------------------------------------------
async def main():
    """
    Orquesta toda la simulación hospitalaria: inicializa recursos, lanza tareas y espera a que todo termine.
    """
    DB_PATH = 'Hospital_Simulacion.db'
    BORRAR_DB_AL_INICIO = True

    # Parámetros de simulación
    lambda_minutos    = 10
    tiempo_simulacion = 30
    num_camas_inicial = 350
    num_medicos_inicial = 80
    max_pac_por_medico = 5
    num_workers_diagnostico = os.cpu_count() or 2

    print(f"--- Iniciando Simulación Hospitalaria ({tiempo_simulacion} segundos) ---")
    print(f"Parámetros: lambda={lambda_minutos}/min, Camas={num_camas_inicial}, Médicos={num_medicos_inicial}, WorkersDiag={num_workers_diagnostico}")
    print(f"BD: {os.path.abspath(DB_PATH)}")

    if BORRAR_DB_AL_INICIO and os.path.exists(DB_PATH):
        print(f"Borrando archivo de BD existente: {DB_PATH}")
        os.remove(DB_PATH)

    # Colas para comunicación entre etapas
    cola_llegadas    = asyncio.Queue()
    cola_prioridad   = asyncio.PriorityQueue()
    cola_asignacion  = asyncio.Queue()
    cola_seguimiento = asyncio.Queue()

    conn = None
    executor = None
    tasks = []

    try:
        # Inicializa la base de datos y recursos
        conn = base_datos_wrapper(DB_PATH)
        cursor = conn.cursor()

        # Crea camas si no existen
        cursor.execute("SELECT COUNT(*) FROM Camas")
        if cursor.fetchone()[0] == 0:
            print(f"Sembrando {num_camas_inicial} camas...")
            cursor.executemany("INSERT INTO Camas (estado) VALUES (?)", [('Disponible',)] * num_camas_inicial)
            conn.commit()
            camas_actuales = num_camas_inicial
        else:
             cursor.execute("SELECT COUNT(*) FROM Camas")
             camas_actuales = cursor.fetchone()[0]
        print(f"Total camas en BD: {camas_actuales}")

        # Crea médicos si no existen
        cursor.execute("SELECT COUNT(*) FROM Medicos")
        if cursor.fetchone()[0] == 0:
            print(f"Sembrando {num_medicos_inicial} médicos...")
            cursor.executemany("INSERT INTO Medicos (pacientes_asignados) VALUES (?)", [(0,)] * num_medicos_inicial)
            conn.commit()
            medicos_actuales = num_medicos_inicial
        else:
             cursor.execute("SELECT COUNT(*) FROM Medicos")
             medicos_actuales = cursor.fetchone()[0]
        print(f"Total médicos en BD: {medicos_actuales}")

        # Inicializa semáforos según recursos disponibles
        cursor.execute("SELECT COUNT(*) FROM Camas WHERE estado='Disponible'")
        camas_disponibles = cursor.fetchone()[0]
        sem_camas = asyncio.Semaphore(camas_disponibles)
        print(f"Semáforo de camas inicializado con {camas_disponibles} permisos.")

        cursor.execute("SELECT SUM(pacientes_asignados) FROM Medicos")
        cupos_ocupados_medicos = cursor.fetchone()[0] or 0
        cupos_totales_medicos = medicos_actuales * max_pac_por_medico
        cupos_disponibles_medicos = max(0, cupos_totales_medicos - cupos_ocupados_medicos)
        sem_medicos = asyncio.Semaphore(cupos_disponibles_medicos)
        print(f"Semáforo de médicos inicializado con {cupos_disponibles_medicos} permisos (cupos).")

        # Executor para diagnóstico paralelo
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers_diagnostico)
        print(f"ProcessPoolExecutor creado con {num_workers_diagnostico} workers.")

        # Lanza las tareas consumidoras
        print(f"[{datetime.datetime.now().isoformat()}] Lanzando tareas consumidoras...")
        task_names = ["Registro", "Diagnostico", "Asignacion", "Seguimiento"]
        consumers = [
            consumidor_registro(cola_llegadas, cola_prioridad, conn),
            consumidor_diagnostico(cola_prioridad, cola_asignacion, conn, executor),
            consumidor_asignacion(cola_asignacion, cola_seguimiento, conn, sem_camas, sem_medicos),
            consumidor_seguimiento(cola_seguimiento, conn, sem_camas, sem_medicos)
        ]
        for name, coro in zip(task_names, consumers):
             tasks.append(asyncio.create_task(coro, name=name))

        print(f"[{datetime.datetime.now().isoformat()}] Lanzando generador de llegadas...")
        lambda_rate = lambda_minutos / 60.0
        t_gen = asyncio.create_task(generador_llegadas(lambda_rate, tiempo_simulacion, cola_llegadas), name="GeneradorLlegadas")
        tasks.append(t_gen)

        print(f"[{datetime.datetime.now().isoformat()}] Simulación en curso... Esperando finalización de llegadas y procesamiento.")

        await t_gen

        await asyncio.gather(
             cola_llegadas.join(),
             cola_prioridad.join(),
             cola_asignacion.join(),
             cola_seguimiento.join()
        )

        print(f"[{datetime.datetime.now().isoformat()}] Todas las colas procesadas.")
        print(f"[{datetime.datetime.now().isoformat()}] Simulación completada.")

    except asyncio.CancelledError:
        print("WARN: La simulación principal fue cancelada.")
    except Exception as e:
        print(f"\nERROR INESPERADO EN MAIN: {e}")
        traceback.print_exc()
        for task in tasks:
             if not task.done():
                 task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if executor:
            print("Cerrando ProcessPoolExecutor...")
            executor.shutdown(wait=True)
            print("Executor cerrado.")
        if conn:
            print("Cerrando conexión a BD...")
            conn.close()
            print("Conexión BD cerrada.")
        print("--- Simulación Finalizada ---")

if __name__ == "__main__":
    # Ejecuta la simulación
    asyncio.run(main())
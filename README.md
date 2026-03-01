### Paso 0 — Asegúrate de estar en la raíz del proyecto

Levanta los contenedores

```
docker compose up -d
```

### 1 - Verifica que todo esté arriba
```
docker ps
```

Deberías ver:

- pt_postgres

- pt_adminer

### 2 - (Opcional pero útil) Entra a Adminer

Abre en tu navegador:

```
http://localhost:8080
```

Y conéctate así:

- System: PostgreSQL

- Server: db (si entras desde el contenedor) o localhost (si entras desde tu PC)

- Username: pt_user

- Password: pt_pass

- Database: pt_db

Verás que todavía no hay tablas pero esas crean a continuación


### 3 - Crear su propio venv
windows:
```
py -3 -m venv .venv
```
Linux:
```
python3 -m venv .venv
```

### Activa el entorno virtual

En PowerShell:
```
.\.venv\Scripts\Activate.ps1
```
En CMD:
```
.venv\Scripts\activate
```
En linux/MAC:
```
source .venv/bin/activate
```

### Actualiza pip dentro del venv

Ya con (.venv) activo:

```
python -m pip install --upgrade pip
```

### Instalar dependencias

```
python -m pip install -r requirements.txt
```
### Verifica que quedaron instaladas
Deberías ver al menos: 
- pandas
- SQLAlchemy
- psycopg2-binary
- python-dotenv

### - Probar la conexión a Postgres
Ahora vamos a verificar que Python sí puede hablar con el contenedor.

Desde la raíz del proyecto:
```
python src/test_connection.py
```
Deberías ver:

Conexión a PostgreSQL exitosa 🚀

# 1.4 Dispersión de la información

# Nota: El diagrama de base de datos resultado esta en /docs y fue generado con:

```
https://dbdiagram.io
```
Con lo siguiente:

```
Table companies {
  company_id text [pk]
  company_name text
}

Table charges {
  charge_id text [pk]
  company_id text
  amount decimal
  status text
  created_at timestamp
  paid_at timestamp
}

Ref: charges.company_id > companies.company_id
```

### — Crear las tablas en PostgreSQL (companies y charges)

7.3 Ejecuta el script

Desde la raíz del proyecto:

```
python src/create_tables.py
```

¿Qué deberías ver?

Un mensaje tipo:
Tablas creadas correctamente...

7.4 Verifica en Adminer

### En Adminer refresca (F5) y ya deberían aparecer:

- companies

- charges

### Tambien puedes ejecutar lo siguiente para ver la conexión y ver si realmente se crearon

```
python src/debug_db.py
```


### Si quieres eliminar las tablas para comenzar de nuevo el proceso
```
python src/reset_db.py
```
# 1.1 Carga de información

### — Cargar data/dataset.csv a companies y charges

- Leer CSV con pandas.

- Separar empresas únicas (company_id, name) y cargarlas a companies.

- Preparar charges (id, company_id, amount, status, created_at, paid_at) y cargarlos a charges.

- Insertar en Postgres de forma segura y rápida usando staging + upsert (para que puedas re-ejecutar).

Convertir tipos:

- amount → número

- created_at, paid_at → fecha/hora

### 7 — Exploración del dataset

###  Ejecutarlo
```
python src/explore_dataset.py
```
Problemas detectados en el dataset

### Valores faltantes
```
id: 3
name: 3
company_id: 4
paid_at: 3991
```
Interpretación:

Campo	Qué hacer

id	❌ eliminar filas

name	❌ eliminar filas

company_id	❌ eliminar filas

paid_at	✔ dejar NULL

### Duplicados
```
Duplicados por id: 2
```
Regla simple:

quedarnos con el primero

### Problema GRAVE en amount

Esto es lo más importante.

```
mean   3e+30
max    3e+34
```

Esto significa que el dataset tiene datos corruptos o overflow.

Regla típica de limpieza:

Eliminar valores absurdos.

Por ejemplo:
```
amount > 100000
```
### Status corruptos
#
```
p&0x3fid
0xFFFF
```
Eso claramente es basura o error de encoding.

### Fechas

Esto también es interesante:

```
created_at max: 20190516
```

Eso es formato incorrecto.

Debe ser:
```
2019-05-16
```

## Conclusión
Antes de cargar los datos realicé un análisis exploratorio del dataset donde detecté valores faltantes, duplicados, montos corruptos y estados inválidos. Implementé un proceso de limpieza que valida claves críticas, filtra anomalías en montos y normaliza campos antes de la carga.

Hay datos extremadamente altos (hasta 10^34), lo cual es inconsistente con montos monetarios.
Para garantizar la calidad de los datos, implementé un filtro de outliers eliminando registros con montos mayores a 100000, en el archivo siguiente:


### Ejecuta la carga
```
python src/load_data.py
```
Qué deberías ver

“✅ Carga completa…”

conteos companies y charges

# Punto 1.2 Extracción

###  — Ejecutar extracción
```
python src/extract_to_file.py
```

Deberías ver algo como:

- “Extracción completada”

- rutas a los archivos

- número de filas exportadas (idealmente 9981)

Vamos a generar dos archivos:

- exports/charges_enriched.parquet ✅ (pro/eficiente)

- exports/charges_enriched.csv ✅ (universal)

## ¿Por qué Parquet?: 
Comprime, guarda tipos bien (fechas/decimales), y es estándar en data engineering.
Por qué CSV también: cualquiera lo abre sin herramientas extra.

# 1.3 Transformación

Ahora toca transformar al esquema “Cargo”con (campos y tipos) y dejarlo listo rchivo transformado (CSV/Parquet)

El dataset trae paid_at, pero el esquema pide updated_at (timestamp NULL).
Lo más razonable (y defendible) es:

- updated_at = paid_at (cuando exista)

- si paid_at es NULL ⇒ updated_at también NULL

### — Ejecuta la transformación
```
python src/transform_to_cargo.py
```

### El conjunto de datos proporcionado utiliza hashes SHA1 (aproximadamente 40 caracteres) para los identificadores Aunque la especificación sugiere varchar(24), truncar los valores provocaría una pérdida de unicidad; por lo tanto, los identificadores se conservaron como cadenas completas.

# 1.5 SQL

Diseña una vista en la base de datos MySQL, Postgres, MongoDB, etc., de las tablas donde cargamos la información transformada para que podamos ver el monto total transaccionado por día para las diferentes compañías

Ese punto pide crear una vista con:

- Monto total transaccionado por día por empresa

### — Ejecuta:
```
python src/create_views.py
```

— Prueba la vista (Adminer → Comando SQL)

Prueba:

```
SELECT * FROM vw_daily_total_by_company LIMIT 10;
```

```
SELECT * FROM vw_daily_paid_total_by_company LIMIT 10;
```

Debe mostrar filas (con totales) para ambos casos
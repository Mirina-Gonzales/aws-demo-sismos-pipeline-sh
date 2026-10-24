# Pipeline de sismos de Sudamérica

Demo para AWS Student Community Day Chile 2026: "Del caos a las respuestas".

Un data lake serverless en AWS que descarga los sismos de Sudamérica publicados por el USGS,
los limpia y los deja listos para consultar con SQL. Arquitectura medallion, orquestada con Step Functions:

```
USGS ──► Lambda ingest ──► S3 bronze ──► Lambda transform ──► Iceberg silver ──► Athena gold
              (JSON crudo)                (pandas + MERGE)                   (vistas SQL)
```

| Capa | Dónde | Qué hay |
|---|---|---|
| Bronze | `s3://sismos-datalake-<cuenta>-<región>/bronze/` | El GeoJSON del USGS tal cual llegó (se borra a los 30 días) |
| Silver | Tabla Iceberg `sismos_db.silver_sismos` | Sismos de Sudamérica limpios, sin duplicados, con su país |
| Gold | Vistas `sismos_db.gold_*` | Respuestas listas: por país, región, día, profundidad y recientes |

**Contenido**

1. [Cómo funciona](#cómo-funciona)
2. [Requisitos](#requisitos)
3. [Credenciales de AWS](#credenciales-de-aws)
4. [Código y ambiente local](#código-y-ambiente-local)
5. [Configuración](#configuración)
6. [Deploy](#deploy)
7. [Crear tablas y vistas](#crear-tablas-y-vistas)
8. [Ejecutar el pipeline](#ejecutar-el-pipeline)
9. [Consultar en Athena](#consultar-en-athena)
10. [Cambiar tablas y vistas](#cambiar-tablas-y-vistas)
11. [Costos y limpieza](#costos-y-limpieza)
12. [Problemas comunes](#problemas-comunes)

---

## Cómo funciona

### Servicios

| Servicio | Recurso | Para qué |
|---|---|---|
| **Amazon S3** | `sismos-datalake-<cuenta>-<región>` | El data lake. Cifrado, sin acceso público, con expiración: bronze 30 días, resultados de Athena 7 días, temporales 1 día |
| **Amazon S3** | `sismos-ddl-<cuenta>-<región>` | Los DDL de las tablas y vistas |
| **AWS Lambda** | `sismos-ingest` | Descarga del USGS y guarda en bronze |
| **AWS Lambda** | `sismos-transform` | Limpia con pandas y hace MERGE en la tabla Iceberg |
| **AWS Step Functions** | `sismos-pipeline` | Orquesta ingesta → limpieza → verificación, con reintentos y modo reproceso |
| **AWS Step Functions** | `sismos-ddl` | Ejecuta en Athena el DDL que le pases |
| **AWS Glue Data Catalog** | Base `sismos_db` | Catálogo de las tablas y vistas |
| **Amazon Athena** | Workgroup `scd-demo` | Motor SQL, con un límite de 1 GB escaneado por query |
| **Amazon EventBridge Scheduler** | `sismos-cada-hora` | Lanza el pipeline cada hora (parte apagado) |
| **Amazon SNS + CloudWatch Alarms** | `sismos-alertas` | Avisan por email si algo falla |
| **AWS IAM** | Un rol por Lambda y por máquina | Cada uno solo con los permisos que necesita |

### Decisiones de diseño

- **Las tablas y vistas solo las crea el flujo de DDL** (`sismos-ddl`), con archivos versionados en `ddl/`.
  La Lambda transform falla si la tabla no existe, y su rol solo puede crear las tablas temporales
  `temp_table_*` que awswrangler usa para el MERGE.
- **MERGE por `id`:** si un sismo ya existe, se actualiza (el USGS corrige magnitudes y ubicaciones);
  si es nuevo, se inserta. Repetir una carga nunca duplica.
- **El deploy es manual y solo desde `main`:** un push no despliega nada. Se lanza desde GitHub Actions.
- **Sin access keys:** GitHub Actions se autentica en AWS con OIDC.

### Estructura del repo

```
src/ingest/          Lambda que descarga del USGS a bronze
src/transform/       Lambda que limpia (limpieza.py, reproceso.py) y hace MERGE en silver
statemachine/        Step Functions: sismos-pipeline y sismos-ddl
ddl/table_<tabla>/   DDL de cada tabla: create y alters, numerados
ddl/view_<vista>/    DDL de cada vista gold
config/demo.toml     Configuración del deploy (schedule, región)
template.yaml        Infraestructura (SAM)
cicd/                Rol OIDC para GitHub Actions
tests/               pytest + fixtures
```

---

## Requisitos

- Una **cuenta de AWS** y una **cuenta de GitHub**.
- macOS con [Homebrew](https://brew.sh). En Linux o Windows, instala los mismos programas con su instalador oficial.

```bash
brew install python@3.12 awscli aws-sam-cli git
```

```bash
python3.12 --version   # 3.12.x
aws --version          # aws-cli/2.32 o superior (necesario para "aws login")
sam --version          # SAM CLI 1.x
```

Todo el proyecto usa la región **`us-east-1`**. Para usar otra, mira [Otra región](#otra-región).

---

## Credenciales de AWS

No uses el usuario root. Crea un usuario IAM y conéctate con `aws login`, que usa tu sesión
de la consola y **no requiere access keys**.

**1. Crear el usuario IAM** (consola de AWS, una vez):

1. **IAM → Users → Create user**. Marca **Provide user access to the AWS Management Console**.
2. Permisos: **Attach policies directly → `AdministratorAccess`**.
   Para una demo es lo más simple, porque SAM crea roles, Lambdas y buckets. En producción usarías permisos acotados.
3. Activa **MFA** para ese usuario (IAM → Users → tu usuario → Security credentials).

**2. Iniciar sesión desde la terminal:**

```bash
aws login --profile scd-login --region us-east-1
```

Se abre el navegador: entra con el usuario IAM y autoriza el acceso.

**3. Crear el perfil que usa el proyecto.** SAM CLI y algunas versiones de boto3 todavía no
entienden el perfil de `aws login`. Por eso un segundo perfil, `scd-demo`, le pide las credenciales al primero.
Deja `~/.aws/config` así:

```ini
[profile scd-login]
login_session = arn:aws:iam::<ID_CUENTA>:user/<TU_USUARIO>
region = us-east-1

[profile scd-demo]
credential_process = aws configure export-credentials --profile scd-login --format process
region = us-east-1
```

La línea `login_session` la escribe `aws login`; tú agregas `region` y el perfil `scd-demo`.

**4. Usar el perfil** (en cada terminal nueva, o agrégalo a tu `~/.zshrc`):

```bash
export AWS_PROFILE=scd-demo
export AWS_DEFAULT_REGION=us-east-1
aws sts get-caller-identity
```

Si dice `Your session has expired`, repite `aws login --profile scd-login`.

---

## Código y ambiente local

Haz un **fork** del repo (lo necesitas propio para GitHub Actions) y clónalo. En los comandos,
`<TU_USUARIO_GITHUB>` es tu usuario y `<TU_REPO>` el nombre de tu fork:

```bash
git clone https://github.com/<TU_USUARIO_GITHUB>/<TU_REPO>.git
cd <TU_REPO>

python3.12 -m venv .venv
source .venv/bin/activate          # actívalo en cada terminal nueva
pip install -r requirements-dev.txt

pytest -v                          # no usan AWS: prueban la limpieza con tests/fixtures/
```

---

## Configuración

SAM lee dos archivos:

- [`template.yaml`](template.yaml): **qué** se crea.
- [`config/demo.toml`](config/demo.toml): **cómo** se despliega. Como no se llama `samconfig.toml`,
  a `sam deploy` siempre se le pasa `--config-file config/demo.toml`.

```toml
parameter_overrides = "AlertEmail=\"tu-correo@ejemplo.com\" ScheduleState=\"DISABLED\""
```

| Parámetro | Para qué |
|---|---|
| `AlertEmail` | El email que recibe las alertas. **Déjalo de ejemplo:** el real se pasa al desplegar (ver [Deploy](#deploy)), así no queda en el repo |
| `ScheduleState` | `ENABLED` o `DISABLED`: la ejecución automática cada hora |

La frecuencia está en `template.yaml`, en el evento `CadaHora` (`ScheduleExpression: rate(1 hour)`).

### Otra región

El template usa la capa pública **AWSSDKPandas** (pandas + awswrangler) publicada por AWS:
`arn:aws:lambda:${AWS::Region}:336392948345:layer:AWSSDKPandas-Python312-Arm64:31`.
Revisa que esa versión exista en tu región en la [lista oficial](https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html),
y cambia `us-east-1` en `config/demo.toml` y en el workflow.

---

## Deploy

El deploy lo hace **GitHub Actions desde tu fork, en tu cuenta de AWS**. El workflow ya viene en el repo
(`.github/workflows/deploy.yml`); lo que cada persona configura es la conexión entre su fork y su cuenta:

| Paso | Dónde | Una vez |
|---|---|---|
| 1. Crear el rol que el workflow puede usar | Tu cuenta de AWS | ✅ |
| 2. Decirle al workflow qué rol usar y a qué email avisar | Tu fork en GitHub | ✅ |
| 3. Habilitar Actions en el fork | Tu fork en GitHub | ✅ |
| 4. Lanzar el deploy | Tu fork en GitHub | Cada vez que quieras desplegar |

### 1. Rol para GitHub Actions

```bash
aws cloudformation deploy \
  --template-file cicd/github-oidc.yaml \
  --stack-name sismos-github-oidc \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides GitHubOwner=<TU_USUARIO_GITHUB> RepoName=<TU_REPO> BranchName=main
```

Crea el proveedor OIDC de GitHub y el rol `sismos-github-deploy`, que **solo puede usar tu fork
desde la rama `main`**: si lanzas el workflow desde otra rama, el deploy se detiene antes de tocar AWS.
`GitHubOwner` y `RepoName` tienen que ser **exactamente** los de tu fork (mira la URL en GitHub):
si no coinciden, el workflow falla con `Not authorized to perform sts:AssumeRoleWithWebIdentity`.
Se puede abrir a cualquier rama con `"BranchName=*"`, pero en un repo público no es recomendable.

Si falla con *Provider already exists*, tu cuenta ya tiene el proveedor OIDC de GitHub (solo puede haber uno):
quita `GitHubOIDCProvider` del template y usa el ARN del existente en `Principal.Federated`.

Copia el ARN del rol:

```bash
aws cloudformation describe-stacks --stack-name sismos-github-oidc \
  --query "Stacks[0].Outputs[?OutputKey=='RoleArn'].OutputValue" --output text
```

### 2. Variable y secret en GitHub

En tu fork → **Settings → Secrets and variables → Actions**:

| Pestaña | Nombre | Valor |
|---|---|---|
| **Variables** | `AWS_ROLE_ARN` | El ARN del paso anterior |
| **Secrets** | `ALERT_EMAIL` | Tu email para las alertas (se oculta en los logs) |

### 3. Habilitar Actions

En los forks, GitHub deja los workflows apagados: pestaña **Actions** →
**I understand my workflows, go ahead and enable them**.

### 4. Lanzar el deploy

1. Sube tus cambios: `git push origin main`. Esto **no despliega**.
2. En GitHub → **Actions → CI/CD pipeline sismos → Run workflow** → rama **`main`** → **Run workflow**.

El workflow corre los tests (si fallan, no despliega), hace `sam build` y `sam deploy` con tu email,
y sube `ddl/` al bucket de DDL. **No ejecuta los DDL:** eso lo haces tú en el paso siguiente.

Después del primer deploy llega un correo **"AWS Notification - Subscription Confirmation"**:
abre **Confirm subscription** o las alertas no te llegan.

<details>
<summary>Deploy desde tu máquina (sin GitHub Actions)</summary>

```bash
sam build
sam deploy --config-file config/demo.toml \
  --parameter-overrides AlertEmail=tu-correo@ejemplo.com ScheduleState=DISABLED
aws s3 sync ddl/ s3://sismos-ddl-<ID_CUENTA>-us-east-1/ --exclude "*" --include "*.sql"
```

`--parameter-overrides` reemplaza **todos** los parámetros de `config/demo.toml`, por eso van los dos.
`sam deploy` muestra el changeset y pide confirmación.
</details>

---

## Crear tablas y vistas

Se crean con la máquina **`sismos-ddl`**: lee un archivo del bucket de DDL y lo ejecuta en Athena.

```
ddl/
├── table_silver_sismos/              tablas: carpeta table_<nombre>
│   ├── 001_create.sql                CREATE TABLE IF NOT EXISTS ...
│   └── 002_add_pais.sql              ALTER TABLE ... ADD COLUMNS (pais)
├── view_gold_sismos_por_pais/        vistas: carpeta view_<nombre>
│   └── 001_create.sql                CREATE OR REPLACE VIEW ...
├── view_gold_sismos_por_region/
│   ├── 001_create.sql
│   └── 002_por_pais.sql              la vista completa, ahora con pais
├── view_gold_sismos_por_dia/
├── view_gold_sismos_por_profundidad/
└── view_gold_sismos_recientes/
```

**Cómo se ejecuta un archivo:** **Step Functions → `sismos-ddl` → Start execution** con el input:

```json
{"carpeta": "table_silver_sismos", "archivo": "001_create.sql"}
```

O desde la terminal:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:<ID_CUENTA>:stateMachine:sismos-ddl \
  --input '{"carpeta": "table_silver_sismos", "archivo": "001_create.sql"}'
```

**En una cuenta nueva, en este orden:**

**1. Crear la tabla silver**, la versión original, sin la columna `pais`:

```json
{"carpeta": "table_silver_sismos", "archivo": "001_create.sql"}
```

**2. Probar el `ALTER`.** La tabla nació sin `pais` a propósito: la columna se agrega con una
migración, como se haría con una tabla que ya tiene datos. Mira las columnas antes y después
en Athena (workgroup `scd-demo`):

```sql
DESCRIBE sismos_db.silver_sismos;
```

Ejecuta el `ALTER`:

```json
{"carpeta": "table_silver_sismos", "archivo": "002_add_pais.sql"}
```

Vuelve a correr el `DESCRIBE`: ahora aparece `pais` al final.

> **¿Y si te lo saltas?** La Lambda transform manda la columna `pais`, así que el pipeline falla
> al escribir en silver porque la tabla no la tiene. Es la razón por la que el `ALTER` va antes de cargar datos.

**3. Crear las vistas gold**, una ejecución por vista:

| carpeta | archivo |
|---|---|
| `view_gold_sismos_por_pais` | `001_create.sql` |
| `view_gold_sismos_por_region` | `002_por_pais.sql` |
| `view_gold_sismos_por_dia` | `002_por_pais.sql` |
| `view_gold_sismos_por_profundidad` | `002_por_pais.sql` |
| `view_gold_sismos_recientes` | `002_por_pais.sql` |

De cada **tabla** se aplican todos sus archivos, en orden. De cada **vista** basta el último,
porque cada archivo trae la vista completa.

---

## Ejecutar el pipeline

**Step Functions → `sismos-pipeline` → Start execution.** El input siempre lleva los mismos 4 parámetros;
el modo lo decide `reproceso`, que está **apagado por defecto**:

| Modo | Input |
|---|---|
| **Normal**: feed de las últimas 24 h → bronze → silver | `{"reproceso": false, "capas": "silver", "start": "AAAA-MM-DD", "end": "AAAA-MM-DD"}` |
| **Reproceso bronze + silver**: vuelve a descargar del USGS el rango → bronze → silver | `{"reproceso": true, "capas": "bronze_silver", "start": "2026-01-01", "end": "2026-10-25"}` |
| **Reproceso solo silver**: rehace silver con lo que ya hay en bronze, día por día | `{"reproceso": true, "capas": "silver", "start": "2026-10-01", "end": "2026-10-25"}` |

- **El schedule manda el input normal.** En ese modo, las fechas `AAAA-MM-DD` se ignoran.
- **Para reprocesar**, cambia `reproceso` a `true` y pon fechas reales: `start` se incluye y `end` no.
  Si las fechas siguen en `AAAA-MM-DD` o falta algo, falla en `ReprocesoInvalido` con un ejemplo.
- **`bronze_silver`** sirve para cargar historia: el USGS guarda todo. Úsalo después de crear las tablas
  para tener datos desde el primer día.
- **`silver`** sirve cuando cambiaste la limpieza y quieres aplicarla a lo ya descargado. Bronze solo guarda 30 días.
- **No lances dos ejecuciones a la vez** (por ejemplo, un reproceso con el schedule encendido):
  dos MERGE simultáneos sobre la misma tabla Iceberg pueden chocar.

```
ModoEjecucion ─┬─ normal ─────────────── Ingest ──────► Transform ──────────────┬─► VerificarSilver
               └─ ValidarReproceso ─┬─ bronze_silver ─► IngestRango ► Transform ┤
                                    └─ silver ────────► ListarDiasBronze        │
                                                        ► ReprocesarDias (Map) ─┘
```

**Ejecución automática:** pon `ScheduleState="ENABLED"` en `config/demo.toml`, haz push y lanza el deploy.
Corre cada hora; vuelve a `DISABLED` cuando no la necesites.

> **Si no aparecen sismos nuevos:** el feed solo trae sismos de magnitud 2,5 o más de las últimas 24 horas,
> y hay días sin ninguno. Carga historia con un reproceso `bronze_silver`.

---

## Consultar en Athena

**Athena → Query editor → workgroup `scd-demo` → pestaña Saved queries.** Las queries guardadas
pertenecen al workgroup: en `primary` no aparecen.

| Query | Qué muestra |
|---|---|
| 01 Panorama general | Total de sismos, países, localidades, rango de fechas y magnitud máxima |
| 02 Donde tiembla mas | Top 10 localidades, con su país |
| 03 Actividad por dia | Sismos por día en toda Sudamérica (fecha de Chile) |
| 04 Durante la charla | Sismos de los últimos 60 minutos |
| 05 Time travel | La tabla hace 30 minutos vs ahora (Iceberg) |
| 06 Sin duplicados | Prueba de que el MERGE no duplica |
| 07 Historial de la tabla | Snapshots de Iceberg (uno por cada MERGE) |
| 08 Por pais | Qué país de Sudamérica tiembla más |

La "05 Time travel" necesita que la tabla tenga al menos 30 minutos.

---

## Cambiar tablas y vistas

**Agregar una columna a silver:**

1. Crea el archivo siguiente en la carpeta de la tabla, por ejemplo `ddl/table_silver_sismos/003_add_intensidad.sql`:
   ```sql
   ALTER TABLE sismos_db.silver_sismos ADD COLUMNS (intensidad double)
   ```
2. Agrega la columna en [`limpieza.py`](src/transform/limpieza.py) (`COLUMNAS_SILVER`) y en sus tests.
3. Haz push y lanza el deploy: sube el archivo al bucket.
4. Ejecuta el `ALTER` con `sismos-ddl` **apenas termine el deploy**, antes de que corra el pipeline:
   la Lambda nueva manda la columna, y si la tabla todavía no la tiene, el MERGE falla.
   Lo más seguro es apagar el schedule durante el cambio.

**Cambiar una vista:** crea el archivo siguiente en su carpeta con el `CREATE OR REPLACE VIEW` completo, y ejecútalo.

**Tabla o vista nueva:** crea `ddl/table_<nombre>/` o `ddl/view_<nombre>/` con su `001_create.sql`.

Reglas:

- **No edites un archivo que ya se ejecutó:** los cambios van en uno nuevo con el número siguiente.
  Así la carpeta es el historial de la tabla o vista.
- **Una sentencia por archivo:** Athena no acepta varias en una misma query.
- **En el `LOCATION` escribe `{{BUCKET_DATALAKE}}`** en vez del nombre del bucket; `sismos-ddl` lo reemplaza.
  Las tablas nuevas van en `silver/<tabla>/`, que es donde la máquina tiene permiso de escribir.
- Los `CREATE TABLE` llevan `IF NOT EXISTS` y las vistas `CREATE OR REPLACE`: se pueden repetir.
  Los `ALTER` no: ejecuta cada uno una sola vez.
- Borrar una tabla (`DROP TABLE`) se hace a mano en Athena: `sismos-ddl` no tiene permiso, a propósito.

<details>
<summary>¿Y en producción?</summary>

- **Registro de lo ejecutado:** una tabla de control (como Flyway o Liquibase) para que el CI aplique
  solo los archivos pendientes, en orden.
- **Revisión obligatoria:** los cambios en `ddl/` por pull request, y un GitHub Environment con aprobación para el deploy.
- **Ambientes separados** (dev, prod), idealmente en cuentas distintas.
- **Lake Formation** para dar permisos por base, tabla o columna.
- **Particionar por mes** (`PARTITIONED BY (month(time_cl))`) en vez de por día: con este volumen, las
  particiones diarias quedan con archivos muy chicos.
- **Mantenimiento de Iceberg:** `OPTIMIZE ... REWRITE DATA USING BIN_PACK` y `VACUUM` programados.
</details>

---

## Costos y limpieza

Todo es serverless y se cobra por uso. Con el schedule apagado, el costo es casi cero; encendido,
son 24 ejecuciones al día de Step Functions, Lambda y Athena.

Para borrar todo:

```bash
# 1. Vaciar los buckets (CloudFormation no borra buckets con objetos)
aws s3 rm s3://sismos-datalake-<ID_CUENTA>-us-east-1 --recursive
aws s3 rm s3://sismos-ddl-<ID_CUENTA>-us-east-1 --recursive

# 2. Borrar el stack del pipeline (también borra la base de Glue y sus tablas)
aws cloudformation delete-stack --stack-name sismos-pipeline

# 3. Borrar el rol de GitHub
aws cloudformation delete-stack --stack-name sismos-github-oidc
```

El stack `aws-sam-cli-managed-default` lo comparten todos tus proyectos SAM: bórralo solo si no usas SAM para nada más.

---

## Problemas comunes

| Error | Causa | Solución |
|---|---|---|
| `Your session has expired` | Venció la sesión de `aws login` | `aws login --profile scd-login` |
| `NoRegion: You must specify a region` | Falta `region` en el perfil `scd-login` | Agrégala en `~/.aws/config` |
| `sam deploy` pide `--stack-name` | Faltó `--config-file` | `sam deploy --config-file config/demo.toml` |
| Actions: `Falta la variable AWS_ROLE_ARN` o `Falta el secret ALERT_EMAIL` | No están configurados en GitHub | [Deploy, paso 2](#2-variable-y-secret-en-github) |
| Actions: `El deploy solo se permite desde main` | Lanzaste el workflow desde otra rama | Haz merge a `main` y lánzalo desde ahí |
| Actions: `Not authorized to perform sts:AssumeRoleWithWebIdentity` | El rol no confía en tu repo o en esa rama | Revisa `GitHubOwner`, `RepoName` y `BranchName` del stack OIDC |
| Hice push y no se desplegó | Es lo esperado: el deploy es manual | **Actions → Run workflow** |
| `sismos-ddl` falla en `InputInvalido` | El input no trae `carpeta` y `archivo` | Usa `{"carpeta": "...", "archivo": "..."}` |
| `sismos-ddl` falla en `LeerDDL` con `NoSuchKey` | El archivo no está en el bucket | Lanza el deploy (sube `ddl/`) y revisa el nombre de carpeta y archivo |
| La Lambda transform falla con `La tabla ... no existe` | Corriste el pipeline antes de crear la tabla | [Crear tablas y vistas](#crear-tablas-y-vistas) |
| Una vista falla con `Table ... does not exist` | Falta crear la tabla | Ejecuta primero `table_silver_sismos` |
| `ICEBERG_TOO_MANY_OPEN_PARTITIONS` | Un MERGE de más de 100 días (versión anterior) | Despliega la versión actual, que hace el MERGE por lotes |
| La query "05 Time travel" falla | La tabla tiene menos de 30 minutos | Espera, o cambia el intervalo a `'5' MINUTE` |
| No llegan las alertas | No confirmaste la suscripción SNS | Revisa tu correo, también en spam |

---

## Datos

Los sismos vienen del [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/)
(Servicio Geológico de los Estados Unidos): el [feed GeoJSON](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
de las últimas 24 horas y la [API de búsqueda FDSN](https://earthquake.usgs.gov/fdsnws/event/1/) para los reprocesos.
Son datos de dominio público; este proyecto no está afiliado al USGS.

Solo incluye sismos de magnitud 2,5 o más que el USGS publica. Para Chile, la fuente oficial
es el [Centro Sismológico Nacional](https://www.csn.uchile.cl/), que registra muchos más sismos pequeños.

## Licencia

[MIT](LICENSE)

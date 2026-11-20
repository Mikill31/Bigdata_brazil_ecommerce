# UNIVERSIDAD SANTO TOMÁS · SECCIONAL TUNJA
**Facultad de Ingeniería de Sistemas**  
**PROYECTO INTEGRADOR — ENTREGA FINAL**  

## Análisis logístico y satisfacción del cliente en el comercio electrónico brasileño con Apache Spark y Streamlit
*Dataset: Olist Brazilian E-Commerce · Arquitectura Medallion (Bronce, Plata en Parquet, Oro) · PySpark, Spark SQL, Estadística Descriptiva, Correlación, Streamlit y Plotly*

* **INTEGRANTES:**
  * Sebastián Felipe Vargas Quintero
  * Néstor Camilo Peña Guarín
  * Luis Carlos Taborda Flórez
* **CURSO:** Big Data 2026-2
* **DOCENTE:** Ing. Martha Susana Contreras Ortiz
* **FECHA DE ENTREGA:** 19 de noviembre de 2026

---

### Resumen
El comercio electrónico en Brasil enfrenta asimetrías logísticas críticas debido a su escala continental y a la concentración de vendedores en la región Sureste, lo que encarece los fletes y prolonga los tiempos de tránsito hacia el Norte y Nordeste, afectando la satisfacción del comprador. Este proyecto construye un flujo de Big Data en Python sobre el dataset público *Olist Brazilian E-Commerce* (**1.550.922 registros**, **52 columnas**, **120,35 MB** en 9 tablas relacionales) bajo una arquitectura *Medallion* (Bronce en CSV, Plata en Parquet y Oro analítica). Mediante **PySpark** y **Spark SQL** se auditaron y limpiaron los datos (24 tipos corregidos, deduplicación por ventanas y reglas de integridad), consolidando una capa Oro de **110.189 ítems entregados válidos**. El análisis estadístico y de correlación de Pearson con **Pandas/NumPy** demuestra que la calificación del cliente se asocia negativamente con la duración del envío ($r = -0,3047$) y los días de retraso ($r = -0,2295$), pero no con el precio ($r = 0,0036$) ni el flete ($r = -0,0331$). Entre los pedidos de 1 estrella, el **32,1 %** llegó con retraso (esperando **35,65 días** en promedio), frente a solo **3,0 %** en los de 5 estrellas (**10,16 días**). Los resultados se desplegaron en un dashboard interactivo web con **Streamlit y Plotly** que integra 5 KPIs, 5 filtros y 7 visualizaciones. *(198 palabras)*.

---

## 1. Introducción

### 1.1. Contexto y problema
* **Sector:** Comercio electrónico (*e-commerce*) y logística de distribución de última milla.
* **Problema:** Olist opera como un *marketplace* brasileño que conecta pequeños y medianos comercios con las principales plataformas de venta en línea del país. Debido a la escala continental de Brasil y a la concentración de los centros de despacho en la región Sureste (São Paulo, Río de Janeiro y Minas Gerais), el transporte hacia las regiones Norte y Nordeste enfrenta fletes elevados y tiempos de tránsito prolongados. Esto deteriora la experiencia del comprador e incrementa las calificaciones negativas (1 y 2 estrellas). La organización requiere cuantificar el desempeño logístico regional, identificar los factores estadísticamente asociados a las malas reseñas y detectar las categorías y periodos críticos de operación.
* **Justificación:** Procesar más de 1,55 millones de registros distribuidos en nueve entidades relacionales exige un motor de procesamiento distribuido como **Apache Spark (PySpark y Spark SQL)** que garantice escalabilidad, trazabilidad por capas y reproducibilidad, complementado con análisis estadístico inferencial/descriptivo y un tablero web interactivo en **Streamlit** para la toma de decisiones operativas.
* **Usuarios de la información:** Gerencia de operaciones y logística, equipos de servicio al cliente (posventa) y vendedores (*sellers*) de la red de Olist que definen promesas de despacho (*SLA*) y tarifas de envío.

### 1.2. Preguntas de análisis
* **P1.** ¿Cuánto tarda la entrega, en días promedio, en cada uno de los 27 estados (`UF`) de Brasil?
* **P2.** ¿Qué factores (retraso frente a la fecha prometida, duración del envío, flete y precio) se asocian a las reseñas de 1 y 2 estrellas?
* **P3.** ¿Qué proporción de los ítems llega con retraso (`entrega_tardia = true`) y cómo cambia esa proporción según la calificación recibida y la evolución mensual?
* **P4.** ¿Qué brecha operativa y económica separa a los estados del Sureste de los del Norte y Nordeste?
* **P5.** ¿Qué categorías de producto presentan mayor incidencia de entregas tardías y baja calificación?

---

## 2. Objetivos

### 2.1. Objetivo general
Diseñar e implementar un flujo completo de Big Data en Python (ingesta, limpieza distribuida con PySpark, almacenamiento columnar en Parquet, procesamiento con Spark SQL, análisis estadístico/correlacional y visualización interactiva en Streamlit y Plotly) sobre el conjunto de datos *Olist Brazilian E-Commerce* para evaluar el impacto del desempeño logístico sobre la satisfacción del cliente.

### 2.2. Objetivos específicos
1. Ingestar y diagnosticar programáticamente las 9 tablas de la capa cruda (**1.550.922 registros**), identificando tipos incorrectos, valores nulos, duplicados por clave primaria, anomalías lógicas y valores atípicos mediante el criterio de rango intercuartílico ($IQR$).
2. Estructurar una arquitectura *Medallion* (Bronce, Plata y Oro) en **PySpark**, aplicando reglas justificadas de limpieza y persistiendo las tablas depuradas en formato **Apache Parquet**.
3. Ejecutar consultas distribuidas mediante **PySpark (`join`, `filter`, `groupBy`)** y **Spark SQL** verificadas con `count()` y `show(n)` para cuantificar tiempos de entrega, tasas de retraso y calificaciones por estado (`UF`), nivel de cumplimiento y categoría de producto.
4. Realizar el análisis estadístico descriptivo (media, desviación estándar, cuartiles $P_{25}, P_{50}, P_{75}, P_{95}$, $IQR$), la matriz de correlación de Pearson y el análisis temporal mensual con **Pandas, NumPy, Matplotlib y Seaborn** para contrastar las hipótesis de negocio.
5. Desplegar una aplicación web interactiva en **Streamlit y Plotly** conectada al repositorio del proyecto, con indicadores clave (KPIs), filtros dinámicos y siete visualizaciones interactivas que respondan a las cinco preguntas de análisis.

---

## 3. Dataset y tratamiento de datos personales (Ley 1581 de 2012)

| Elemento | Detalle |
| :--- | :--- |
| **Fuente** | Kaggle – *Brazilian E-Commerce Public Dataset by Olist* (`olistbr/brazilian-ecommerce`), listada como fuente sugerida en la guía del proyecto. |
| **Enlace** | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| **Fecha de descarga** | 28 de octubre de 2026 (actualizado y verificado en repositorio el 19 de noviembre de 2026) |
| **Licencia** | **CC BY-NC-SA 4.0** (*Creative Commons Reconocimiento-NoComercial-CompartirIgual 4.0*). Permite el uso y la transformación con fines académicos. |
| **Tablas integradas** | 9 tablas relacionales: `orders`, `order_items`, `order_reviews`, `customers`, `products`, `sellers`, `order_payments`, `geolocation` y `product_category_name_translation`. |
| **Volumen global** | **1.550.922 filas**, **52 columnas** y **120,35 MB** en la capa cruda (supera ampliamente el mínimo exigido de 100.000 registros). El detalle por tabla está en la Tabla 1 y el diccionario de datos completo en el Anexo B. |
| **Consideraciones de datos personales (Ley 1581 de 2012)** | Cumplimiento estricto del régimen de protección de datos personales (**Ley 1581 de 2012** de Colombia y **LGPD** de Brasil): el dataset no contiene nombres propios, documentos de identidad, correos electrónicos, teléfonos ni direcciones postales exactas. Los identificadores de clientes (`customer_id`, `customer_unique_id`) y vendedores (`seller_id`) están **seudonimizados en origen mediante hashes criptográficos MD5 de 32 caracteres**. Asimismo, la ubicación geográfica se trunca a los primeros 5 dígitos del código postal (`zip_code_prefix`) y en la capa Plata se agrupa por centroide promedio, impidiendo cualquier reidentificación individual. |

**Tabla 1.** *Volumen de la capa cruda por tabla (salida de la sección 4 del notebook).*

| Tabla | Archivo CSV en `data/raw/` | Filas | Columnas | Tamaño (MB) | Responsable de la fuente (Electiva B) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `customers` | `olist_customers_dataset.csv` | 99.441 | 5 | 8,62 | Sebastián Felipe Vargas Quintero |
| `orders` | `olist_orders_dataset.csv` | 99.441 | 8 | 16,84 | Sebastián Felipe Vargas Quintero |
| `items` | `olist_order_items_dataset.csv` | 112.650 | 7 | 14,72 | Néstor Camilo Peña Guarín |
| `payments` | `olist_order_payments_dataset.csv` | 103.886 | 5 | 5,51 | Luis Carlos Taborda Flórez |
| `reviews` | `olist_order_reviews_dataset.csv` | 99.224 | 7 | 13,78 | Néstor Camilo Peña Guarín |
| `products` | `olist_products_dataset.csv` | 32.951 | 9 | 2,27 | Sebastián Felipe Vargas Quintero |
| `sellers` | `olist_sellers_dataset.csv` | 3.095 | 4 | 0,17 | Néstor Camilo Peña Guarín |
| `geolocation` | `olist_geolocation_dataset.csv` | 1.000.163 | 5 | 58,44 | Sebastián Felipe Vargas Quintero |
| `translation` | `product_category_name_translation.csv` | 71 | 2 | < 0,01 | Sebastián Felipe Vargas Quintero |
| **TOTAL** | **9 archivos CSV** | **1.550.922** | **52** | **120,35** | **Equipo Big Data 2026-2** |

---

## 4. Arquitectura del flujo de datos y temáticas del syllabus aplicadas

### 4.1. Arquitectura de datos (Patrón Medallion — Electiva B)
El flujo implementa físicamente la arquitectura **Medallion (Lakehouse por capas)** dentro del repositorio: los datos crudos se conservan intactos en la **Capa Bronce**, se depuran y tipifican en formato columnar en la **Capa Plata**, y se integran y agregan para el consumo analítico y estadístico en la **Capa Oro**.

| FUENTE: Kaggle (9 archivos CSV · Olist) | ▶ | CAPA BRONCE: 9 CSV crudos sin modificar (`/data/raw`) | ▶ | CAPA PLATA: 9 tablas Parquet limpias y tipadas (`/data/clean`) | ▶ | CAPA ORO: `olist_oro` (110.189 ítems) + 8 CSV analíticos (`/docs`) | ▶ | CONSUMO WEB: Dashboard Streamlit + Plotly (`/app/app.py`) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |

* **Capa Bronce (`data/raw/`):** Conserva las 9 tablas CSV originales (`120,35 MB`, `1.550.922` filas) sin alteraciones como evidencia reproducible de la fuente.
* **Capa Plata (`data/clean/`):** Diccionario `clean` con las 9 tablas depuradas (tipos casteados, duplicados eliminados, ciudades normalizadas, nulos tratados y variables derivadas) persistidas en formato columnar **Apache Parquet**.
* **Capa Oro (`data/clean/olist_oro` y `docs/`):** Tabla analítica integrada `df_oro` con **110.189 ítems entregados válidos** y las tablas de resumen exportadas a `/docs` (`olist_resumen_estados.csv`, `olist_resumen_reviews_sql.csv`, `olist_resumen_categorias_sql.csv`, `olist_estadistica_descriptiva.csv`, `olist_correlacion.csv`, `olist_serie_mensual.csv` y `olist_resumen_pagos.csv`).
* **Capa de Consumo (`app/app.py`):** Aplicación web interactiva en **Streamlit y Plotly** desplegada a partir del repositorio de GitHub que consume directamente los insumos de la capa Oro.

### 4.2. Articulación con las temáticas del syllabus

| Tipo | Eje temático del syllabus | Implementación real en el proyecto |
| :--- | :--- | :--- |
| **Obligatoria 1** | **Unidad 3: Procesamiento con Apache Spark, PySpark y Spark SQL** | Lectura distribuida de las 9 tablas crudas, funciones de ventana (`Window.partitionBy`), `join` relacional de 5 tablas para construir la capa Oro (`110.189` filas), filtros (`filter`), agregaciones (`groupBy`), vista temporal `olist_oro`, dos consultas en **Spark SQL** y verificación sistemática con `count()` y `show(n)`. |
| **Obligatoria 2** | **Unidad 5: Python para análisis y representación de datos (Pandas, NumPy, Matplotlib/Seaborn, Plotly, Streamlit, estadística y correlación)** | Limpieza documentada con métricas antes/después; cálculo de estadística descriptiva (media, DE, cuartiles, $IQR$) y matriz de correlación de Pearson con **Pandas y NumPy**; gráficos estadísticos en el notebook con **Matplotlib y Seaborn** (heatmap, boxplot, serie mensual dual); y dashboard web en **Streamlit con 7 gráficos interactivos de Plotly**, 5 KPIs y 5 filtros. |
| **Electiva B** | **Unidades 1 y 4: Arquitectura y gobierno de datos (Capas Bronce/Plata/Oro)** | Diagrama de arquitectura Medallion, organización física de los datos en capas (`data/raw`, `data/clean`, `docs`), diccionario de datos completo de las 52 columnas con responsable asignado por fuente (`docs/diccionario_datos.csv`) y cumplimiento de la Ley 1581 de 2012. |

---

## 5. Limpieza, transformación y métricas de calidad antes y después

### 5.1. Diagnóstico automatizado de calidad
El diagnóstico se ejecutó por código en las secciones 4 a 8 del notebook:
* **Tipos incorrectos:** Todas las columnas se leyeron inicialmente como `string` para auditar anomalías de formato mediante expresiones regulares y luego se convirtieron explícitamente **24 columnas** a `timestamp`, `double` e `integer` (`orders`: 5, `items`: 4, `payments`: 3, `reviews`: 3, `products`: 7, `geolocation`: 2). Los prefijos de código postal se conservaron como texto de 5 caracteres con `lpad`.
* **Valores nulos:** En `orders` existen `2.965` nulos (`2,98 %`) en `order_delivered_customer_date` y `1.783` (`1,79 %`) en `order_delivered_carrier_date`, correspondientes a pedidos aún no entregados o cancelados. En `products`, `610` productos (`1,85 %`) no tenían categoría asignada y `2` no tenían dimensiones. En `reviews`, los títulos (`88,34 %`) y comentarios (`58,71 %`) nulos son opcionales y legítimos.
* **Duplicados:** En `reviews`, la deduplicación por clave primaria `review_id` conservando la respuesta más reciente redujo las filas de `99.224` a `98.410` (`814` duplicados eliminados). En `geolocation` se detectaron `261.831` filas duplicadas exactas y `981.148` claves repetidas sobre `1.000.163` registros.
* **Inconsistencias lógicas:** Se detectaron `8` pedidos con estado `delivered` pero sin fecha de entrega al cliente; `42` coordenadas fuera del *bounding box* territorial de Brasil (latitud fuera de $[-34^\circ, 6^\circ]$ o longitud fuera de $[-74^\circ, -34^\circ]$); `3` pagos con `payment_type = not_defined`; y `4` productos con peso igual a `0 g`.
* **Categorías mal escritas:** Normalizar minúsculas, tildes y espacios redujo las variantes de ciudades en `sellers` de `611` a `608` y en `geolocation` de `8.011` a `5.967` (`2.044` variantes sobrantes eliminadas). En `translation` se agregaron `2` categorías faltantes (`pc_gamer` y `portateis_cozinha_e_preparadores_de_alimentos`), pasando de `71` a `73` categorías traducidas.
* **Valores atípicos ($IQR$):** Con la regla $1,5 \times IQR$ se identificaron `8.427` valores extremos en `price` y `12.123` en `freight_value`. No se eliminaron porque corresponden a transacciones reales de electrodomésticos, tecnología o mobiliario pesado.

### 5.2. Decisiones técnicas de limpieza y justificación

| Tabla | Decisión técnica aplicada en PySpark | Justificación |
| :--- | :--- | :--- |
| **Todas** | Leer en crudo como texto y castear explícitamente 24 columnas numéricas y de fecha | Permite auditar formatos corruptos antes de convertir y operar matemáticamente sin errores silenciosos. |
| **Todas** | Eliminar duplicados exactos y por clave primaria (`order_id`, `order_item_id`, `review_id`, `product_id`, `seller_id`) | Evita sobreestimar ventas, conteos de ítems o distorsionar promedios de calificación. |
| **`reviews`** | Conservar únicamente la reseña más reciente por `review_id` usando ventana `Window.partitionBy("review_id")` | Existen `814` reseñas repetidas en varios pedidos; la marca temporal más reciente refleja la evaluación definitiva. |
| **`orders`** | Conservar nulos en fechas de pedidos no entregados y crear la bandera booleana `fecha_inconsistente` | Un pedido `canceled` o `shipped` legítimamente carece de fecha de entrega final. La bandera filtra los `8` casos `delivered` sin fecha. |
| **`payments`** | Filtrar y descartar los 3 registros con `payment_type == 'not_defined'` | No constituyen un medio de pago válido dentro del modelo transaccional (`103.886` $\rightarrow$ `103.883` filas). |
| **`products` y `translation`** | Corregir ortografía `lenght` $\rightarrow$ `length`; imputar categoría nula como `sin_categoria`; pasar peso `0` a `null`; añadir 2 traducciones | Corrige errores tipográficos de origen, evita perder `610` productos al hacer `join` y anula pesos físicamente imposibles (`0 g`). |
| **`geolocation`** | Filtrar `42` coordenadas fuera de Brasil y agrupar por `geolocation_zip_code_prefix` promediando latitud y longitud | Reduce `1.000.163` registros redundantes a `19.010` centroides postales únicos, evitando explosión cartesiana en cruces espaciales. |
| **`items` (Atípicos)** | Conservar los valores extremos de `price` y `freight_value` reportando mediana e $IQR$ | Son ventas legítimas de alto ticket; eliminarlas sesgaría el ingreso total (`R$ 15,42 M`) y el análisis de fletes pesados. |

### 5.3. Tabla comparativa de métricas de calidad antes y después

**Tabla 2.** *Métricas de calidad antes y después de la limpieza en PySpark (salida de la sección 8 del notebook / `docs/metricas_antes_despues.csv`).*

| Tabla | Filas antes | Filas después | Nulos antes | Nulos después | Duplicadas exactas antes | Duplicadas exactas después | Tipos corregidos |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `customers` | 99.441 | 99.441 | 0 | 0 | 0 | 0 | 0 |
| `orders` | 99.441 | 99.441 | 4.908 | 4.908 | 0 | 0 | 5 |
| `items` | 112.650 | 112.650 | 0 | 0 | 0 | 0 | 4 |
| `payments` | 103.886 | 103.883 | 0 | 0 | 0 | 0 | 3 |
| `reviews` | 99.224 | 98.410 | 145.903 | 144.644 | 0 | 0 | 3 |
| `products` | 32.951 | 32.951 | 2.448 | 1.842 | 0 | 0 | 7 |
| `sellers` | 3.095 | 3.095 | 0 | 0 | 0 | 0 | 0 |
| `geolocation` | 1.000.163 | 19.010 | 0 | 0 | 261.831 | 0 | 2 |
| `translation` | 71 | 73 | 0 | 0 | 0 | 0 | 0 |
| **TOTAL** | **1.550.922** | **568.954** | **153.259** | **151.394** | **261.831** | **0** | **24** |

---

## 6. Procesamiento distribuido con PySpark y Spark SQL

### 6.1. Integración de la Capa Oro (`join` relacional de 5 tablas)
En la sección 10.1 del notebook, se consolidó la tabla analítica `df_oro` uniendo `items` con `orders` (`inner`), con el promedio de `reviews` por pedido (`left`), con `customers` (`inner`) y con `products` enriquecida con `translation` (`left`). Se filtraron únicamente los pedidos con `order_status == 'delivered'` y `fecha_inconsistente == False`, y se calcularon las variables `dias_entrega`, `dias_retraso`, `entrega_tardia` y `valor_total_item`.
* **Verificación con `count()`:** `df_oro.count()` devuelve exactamente **110.189 ítems entregados válidos** (superando el umbral de 100.000 registros en la propia capa analítica).

### 6.2. Consulta 1 — Filtro de casos críticos (`filter` + `count()`)
```python
df_criticos = df_oro.filter((F.col("entrega_tardia") == True) & (F.col("review_score") <= 2))
print(df_criticos.count())  # Salida verificada: 4,620
```
* **Resultado verificado:** Se identificaron **4.620 ítems críticos** (`4,19 %` del total de la capa Oro) que combinan entrega tardía y calificación de 1 o 2 estrellas, con demoras observadas de hasta 49 días y retrasos de hasta 22 días sobre la fecha prometida en estados como `BA`, `RJ`, `RS`, `AL` y `MG`.

### 6.3. Consulta 2 — Desempeño logístico y satisfacción por Estado (`groupBy` + `count()`)
La agregación `df_oro.groupBy("customer_state")` verificada con `df_estados.count() == 27` cuantifica los indicadores para las 27 unidades federativas de Brasil:
* **São Paulo (`SP`)** concentra **46.441 ítems** (`42,15 %` de la capa Oro) y registra el menor tiempo medio de entrega con **8,66 días**, flete promedio de **R\$ 15,17**, tasa de entrega tardía de **5,39 %** y calificación de **4,20 ⭐**.
* En el extremo opuesto, **Amapá (`AP`)** y **Roraima (`RR`)** presentan los mayores tiempos de tránsito con **28,22 días** y **28,17 días** respectivamente (`RR` paga el flete más costoso del país: **R\$ 43,09**, junto con Paraíba `PB`).
* **Alagoas (`AL`)** sufre la mayor tasa de entregas tardías del país (**24,12 %**, `24,45` días promedio y calificación de **3,84 ⭐**), seguido por **Maranhão (`MA`)**, que registra la calificación promedio más baja de Brasil (**3,75 ⭐**) y el segundo mayor incumplimiento (**20,38 %** de entregas tardías).

### 6.4. Consulta 3 — Factores asociados a las malas reseñas (`Spark SQL`)
Ejecutada sobre la vista temporal `olist_oro` agrupando por `CAST(ROUND(review_score) AS INT)` y `estado_cumplimiento` (`count() == 10` combinaciones sobre **108.829 ítems con reseña**; `1.360` ítems equivalentes al `1,23 %` no tienen reseña).

**Tabla 3.** *Calificación frente a cumplimiento logístico (resultado de la Consulta 3 en Spark SQL).*

| Calificación | Cumplimiento | Ítems | % dentro de su calificación | Días de entrega promedio | Días de retraso promedio | Flete promedio (BRL) | Precio promedio (BRL) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **1 estrella** | Con Retraso | 3.966 | **32,12 %** | **35,65** | **+11,67** | 22,86 | 139,95 |
| **1 estrella** | A Tiempo | 8.381 | 67,88 % | 11,95 | −14,24 | 20,25 | 115,83 |
| **2 estrellas** | Con Retraso | 654 | **17,90 %** | **32,69** | **+9,44** | 22,09 | 123,51 |
| **2 estrellas** | A Tiempo | 2.999 | 82,10 % | 12,07 | −13,88 | 20,72 | 112,48 |
| **3 estrellas** | Con Retraso | 946 | 10,34 % | 29,59 | +7,01 | 22,30 | 125,05 |
| **3 estrellas** | A Tiempo | 8.205 | 89,66 % | 12,17 | −13,16 | 20,07 | 108,00 |
| **4 estrellas** | Con Retraso | 1.019 | 4,85 % | 26,73 | +5,87 | 20,89 | 123,59 |
| **4 estrellas** | A Tiempo | 19.978 | 95,15 % | 11,40 | −13,40 | 20,01 | 118,53 |
| **5 estrellas** | Con Retraso | 1.877 | **2,99 %** | 24,32 | +4,30 | 20,89 | 127,14 |
| **5 estrellas** | A Tiempo | 60.804 | **97,01 %** | **10,16** | **−13,96** | 19,54 | 121,27 |
| **Total con reseña** | — | **108.829** | **100 %** | — | — | — | — |

### 6.5. Consulta 4 — Categorías críticas de producto (`Spark SQL`)
La consulta en Spark SQL con cláusula `HAVING COUNT(*) >= 200` devuelve **45 categorías representativas** (`count() == 45`). Las categorías peor calificadas son:
1. **`office_furniture`** (`1.647` ítems): Calificación más baja del catálogo (**3,51 ⭐**), tiempo medio de entrega de **20,77 días** (el más alto entre todas las categorías), **8,86 %** de entregas tardías y ventas por **R\$ 330.304,38**.
2. **`fixed_telephony`** (`252` ítems): Calificación de **3,76 ⭐**, `12,56` días de entrega y `3,97 %` de retraso.
3. **`audio`** (`358` ítems): Calificación de **3,84 ⭐**, `13,26` días de entrega y la mayor tasa de entregas tardías del grupo (**12,57 %**).
4. **`home_confort`** (`426` ítems): Calificación de **3,87 ⭐**, `13,49` días de entrega y **10,33 %** de entregas tardías.
5. **`bed_bath_table`** (`10.718` ítems, la categoría de mayor volumen con **R\$ 1.199.833,76**): Calificación de **3,93 ⭐**, `12,67` días de entrega y `8,20 %` de retraso.

---

## 7. Análisis exploratorio, estadístico y de correlación (Unidad 5)

Para completar la **Temática Obligatoria 2 (Unidad 5)** y la tarea **T7**, en la Parte 3 del notebook (secciones 13 a 15) se procesó la capa Oro (`n = 110.189`) con **Pandas, NumPy, Matplotlib y Seaborn**.

### 7.1. Estadística descriptiva y asimetría de distribuciones

**Tabla 4.** *Estadística descriptiva de las variables numéricas de la capa Oro (salida de la sección 13.1 del notebook / `docs/olist_estadistica_descriptiva.csv`).*

| Variable | $n$ válido | Media | Desv. Estándar | Mínimo | $P_{25}$ (Q1) | Mediana ($P_{50}$) | $P_{75}$ (Q3) | $P_{95}$ | Máximo | $IQR$ ($Q_3 - Q_1$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `price` (BRL) | 110.189 | 119,98 | 182,30 | 0,85 | 39,90 | **74,90** | 134,17 | 349,00 | 6.735,00 | 94,27 |
| `freight_value` (BRL) | 110.189 | 19,95 | 15,70 | 0,00 | 13,08 | **16,26** | 21,15 | 45,09 | 409,68 | 8,07 |
| `valor_total_item` (BRL) | 110.189 | 139,93 | 189,32 | 6,08 | 55,18 | **92,12** | 157,47 | 375,26 | 6.929,31 | 102,29 |
| `product_weight_g` (g) | 110.163 | 2.089,84 | 3.741,78 | 2,00 | 300,00 | **700,00** | 1.800,00 | 9.750,00 | 40.425,00 | 1.500,00 |
| `dias_entrega` (días) | 110.189 | 12,41 | 9,45 | 0,00 | 7,00 | **10,00** | 16,00 | 29,00 | 210,00 | 9,00 |
| `dias_retraso` (días) | 110.189 | −12,03 | 10,16 | −147,00 | −17,00 | **−13,00** | −7,00 | +3,00 | +188,00 | 10,00 |
| `review_score` (1 a 5 ⭐) | 108.829 | 4,08 | 1,34 | 1,00 | 4,00 | **5,00** | 5,00 | 5,00 | 5,00 | 1,00 |

**Hallazgos estadísticos con cifras:**
1. **Asimetría positiva en precios, pesos y tiempos de entrega:** En `dias_entrega`, la media (**12,41 días**) supera a la mediana (**10,00 días**), con el 50 % central de los envíos entre **7,00 y 16,00 días** ($IQR = 9,00$ días), pero el percentil 95 alcanza **29,00 días** y el máximo llega a **210,00 días** debido a la cola larga de envíos hacia el Norte y Nordeste.
2. **Holgura conservadora en la fecha estimada (`dias_retraso`):** La media de `dias_retraso` es de **−12,03 días** (mediana **−13,00 días**), lo que significa que Olist fija fechas estimadas de entrega con cerca de dos semanas de colchón. Solo a partir del percentil 95 (**+3,00 días**) se observan incumplimientos de la fecha límite (`7,77 %` de los ítems con reseña tienen `entrega_tardia = True`).
3. **Concentración de calificaciones:** `review_score` tiene media de **4,08 ⭐** y mediana de **5,00 ⭐** ($P_{25} = 4,00$), mostrando una distribución bimodal o sesgada hacia 5 estrellas (`57,6 %` de los ítems), donde los casos de 1 estrella (`11,35 %`) actúan como indicador crítico de falla operativa.

### 7.2. Análisis de correlación de Pearson

**Tabla 5.** *Matriz de correlación lineal de Pearson sobre la capa Oro ($n = 110.189$, salida de la sección 13.2 del notebook / `docs/olist_correlacion.csv`).*

| Variable | `price` | `freight_value` | `valor_total_item` | `product_weight_g` | `dias_entrega` | `dias_retraso` | `review_score` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`price`** | 1,0000 | 0,4127 | 0,9971 | 0,3384 | 0,0627 | −0,0036 | **0,0036** |
| **`freight_value`** | 0,4127 | 1,0000 | 0,4803 | **0,6103** | 0,2148 | −0,0403 | **−0,0331** |
| **`valor_total_item`** | 0,9971 | 0,4803 | 1,0000 | 0,3712 | 0,0785 | −0,0068 | **0,0007** |
| **`product_weight_g`** | 0,3384 | **0,6103** | 0,3712 | 1,0000 | 0,0870 | 0,0115 | −0,0288 |
| **`dias_entrega`** | 0,0627 | 0,2148 | 0,0785 | 0,0870 | 1,0000 | **0,5974** | **−0,3047** |
| **`dias_retraso`** | −0,0036 | −0,0403 | −0,0068 | 0,0115 | **0,5974** | 1,0000 | **−0,2295** |
| **`review_score`** | **0,0036** | **−0,0331** | **0,0007** | −0,0288 | **−0,3047** | **−0,2295** | 1,0000 |

**Interpretación crítica de las correlaciones:**
* **Confirmación estadística para P2:** La calificación del cliente (`review_score`) presenta su mayor correlación negativa con **`dias_entrega` ($r = -0,3047$)** y con **`dias_retraso` ($r = -0,2295$)**. Por el contrario, la correlación con el **precio del producto es prácticamente cero ($r = 0,0036$)** y con el **costo del flete es marginal ($r = -0,0331$)**. Esto demuestra empíricamente que el comprador brasileño no castiga la calificación porque un producto o su flete sean costosos, sino porque el pedido tarda demasiado o incumple la fecha prometida.
* **Determinantes del flete:** El costo de envío (`freight_value`) se correlaciona fuertemente con el **peso del producto en gramos ($r = 0,6103$)**, moderadamente con el precio ($r = 0,4127$) y positivamente con los días de entrega ($r = 0,2148$, reflejo de la distancia geográfica).

### 7.3. Comparación entre periodos mensuales y estructura de pagos
El análisis de la serie mensual (`docs/olist_serie_mensual.csv`, 20 meses entre enero de 2017 y agosto de 2018) y de la tabla limpia de pagos (`docs/olist_resumen_pagos.csv`, `103.883` transacciones) reveló dos dinámicas estructurales:
1. **Choques estacionales y logísticos (2017–2018):** Entre enero y octubre de 2017 la tasa de entregas tardías se mantuvo controlada entre **2,85 % y 7,51 %**, con calificaciones promedio entre **4,10 ⭐ y 4,29 ⭐**. Sin embargo, en **noviembre de 2017 (*Black Friday*)**, el volumen saltó un 62,5 % en un solo mes (de `5.214` a **8.474 ítems** y **R\$ 1.153.229,37**), duplicando los retrasos a **13,83 %** y haciendo caer la calificación a **3,90 ⭐**. Posteriormente, en **febrero y marzo de 2018** (congestión postal y huelgas de transporte en Brasil), la tasa de entregas tardías alcanzó su pico histórico de **15,74 %** (`16,84` días de entrega) y **20,47 %** (`15,97` días), desplomando la calificación media a **3,78 ⭐** y **3,75 ⭐**. En junio–julio de 2018 la operación se estabilizó (`1,34 %` y `4,37 %` de retraso, con tiempos de `9,04` y `8,81` días), recuperando la satisfacción a **4,21 ⭐ y 4,28 ⭐**.
2. **Estructura financiera de pagos:** De un total de **R\$ 16.008.872,12** procesados en `103.883` registros de pago válidos, la **tarjeta de crédito (`credit_card`)** domina con **76.795 transacciones** (**78,34 %** del valor total, **R\$ 12.542.084,19**), un ticket promedio de **R\$ 163,32** y un financiamiento medio de **3,51 cuotas**. Le siguen el **boleto bancario (`boleto`)** con **19.784 transacciones** (**17,92 %** del valor, **R\$ 2.869.361,27**, pago de contado), los **cupones (`voucher`)** con **2,37 %** (`5.775` transacciones, ticket de `R$ 65,70`) y la **tarjeta débito (`debit_card`)** con **1,36 %** (`1.529` transacciones).

---

## 8. Dashboard interactivo en Streamlit y Plotly

La aplicación web ([app/app.py](file:///c:/Users/sebastian/Downloads/proyecto_bigdata/app/app.py)) supera ampliamente los requisitos mínimos de la rúbrica final (que exige KPIs, mínimo 4 gráficos de Plotly y al menos 2 filtros interactivos), integrando **5 KPIs ponderados**, **5 filtros interactivos** en la barra lateral y **7 visualizaciones interactivas de Plotly** organizadas en 4 pestañas temáticas:

### 8.1. Filtros interactivos e indicadores clave (KPIs)
* **5 Filtros en la barra lateral:** (1) Selector múltiple de **Macro-Región** (`Sureste`, `Sur`, `Centro-Oeste`, `Nordeste`, `Norte`); (2) Selector múltiple de **Estados (`UF`)**; (3) Slider de **Rango de días promedio de entrega** (`0,0` a `30,0` días); (4) Selector de **Estado de Cumplimiento Logístico** (`A Tiempo` / `Con Retraso`); y (5) Slider de **Volumen mínimo de ítems por categoría** (`200` a `3.000`).
* **5 KPIs dinámicos ponderados:** (1) *Ítems Entregados Analizados* (**110.189**); (2) *Tiempo Promedio de Entrega* (**12,41 días**, mediana `10,0` días); (3) *Tasa de Entrega Tardía* (**7,91 %**, con `4.620` casos críticos); (4) *Satisfacción Promedio* (**4,08 ⭐**); y (5) *Volumen de Ventas* (**R\$ 15.419.131**, flete promedio **R\$ 19,95**).

### 8.2. Descripción de las 7 visualizaciones de Plotly y preguntas que responden

| Visualización en Streamlit (`app/app.py`) | Tipo de gráfico Plotly | Pregunta que responde y hallazgo visualizado |
| :--- | :--- | :--- |
| **Gráfico 1 · Tiempo de Entrega y Satisfacción por Estado (`UF`)** | Barras horizontales (`px.bar`) con escala de color continua `RdYlGn` y línea de media nacional | **Responde P1 y P4:** Ordena los 27 estados desde `SP` (`8,66 d`, verde `4,20 ⭐`) hasta `AP` (`28,22 d`) y `RR` (`28,17 d`), destacando en rojo los estados críticos del Nordeste (`MA` `3,75 ⭐` y `AL` `3,84 ⭐`). |
| **Gráfico 2 · Costo de Flete vs. Tiempo de Entrega por Macro-Región** | Dispersión de burbujas (`px.scatter`) dimensionado por volumen de ítems | **Responde P4:** Muestra la separación espacial entre el clúster del Sureste (abajo a la izquierda: bajo flete `R$ 15–21` y entrega rápida `8–15 d`) y los clústeres del Norte y Nordeste (arriba a la derecha: fletes de `R$ 32–43` y demoras de `19–28 d`). |
| **Gráfico 3 · Evolución Mensual de Entregas Tardías (%) vs. Calificación (2017–2018)** | Gráfico combinado de doble eje (`go.Figure`: barras de retraso + línea de score) | **Responde P3:** Evidencia cómo los picos de retraso en *Black Friday* (`nov-2017: 13,83 %`) y `feb-mar 2018` (`15,74 %` y `20,47 %`) provocan caídas inmediatas en la curva de calificación hasta `3,75 ⭐`. |
| **Gráfico 4 · Días de Entrega según Calificación y Cumplimiento** | Barras agrupadas (`px.bar`, `barmode="group"`) por score de 1 a 5 estrellas | **Responde P2 y P3:** Contrasta la duración media de los pedidos `Con Retraso` (`35,65 d` en 1⭐ a `24,32 d` en 5⭐) frente a los entregados `A Tiempo` (`11,95 d` en 1⭐ a `10,16 d` en 5⭐). |
| **Gráfico 5 · Categorías con Menor Satisfacción e Incidencia de Retraso** | Barras horizontales (`px.bar`) coloreadas por `% entrega tardía` (`OrRd`) | **Responde P5:** Identifica el Top 15 de categorías críticas encabezadas por `office_furniture` (`3,51 ⭐`, `20,77 d`), `fixed_telephony` (`3,76 ⭐`), `audio` (`3,84 ⭐`, `12,57 %` tardía) y `home_confort` (`3,87 ⭐`). |
| **Gráfico 6 · Matriz de Correlación de Pearson (Capa Oro)** | Mapa de calor interactivo (`px.imshow`) con coeficientes anotados | **Responde P2:** Permite inspeccionar interactivamente las correlaciones entre precio, flete, peso, días de entrega, retraso y calificación ($r = -0,3047$). |
| **Gráfico 7 · Distribución del Valor por Método de Pago** | Barras verticales (`px.bar`) con etiquetas de participación porcentual | **Complemento financiero:** Muestra el predominio de la tarjeta de crédito (`78,34 %`, `3,51` cuotas promedio) y el boleto bancario (`17,92 %`). |

---

## 9. Conclusiones, limitaciones y recomendaciones

### 9.1. Respuesta sintetizada a las preguntas de análisis
* **Respuesta a P1 (Tiempos de entrega por estado):** El promedio nacional ponderado es de **12,41 días** (mediana **10,00 días**). El estado más rápido es **São Paulo (`8,66` días)**, seguido de **Paraná (`11,89` días)** y **Minas Gerais (`11,92` días)**. Los más lentos son **Amapá (`28,22` días)**, **Roraima (`28,17` días)**, **Amazonas (`26,34` días)**, **Alagoas (`24,45` días)** y **Pará (`23,70` días)**.
* **Respuesta a P2 (Factores asociados a las malas reseñas):** El análisis de correlación de Pearson ($n = 110.189$) confirma que los factores determinantes de una mala calificación son la **duración total del envío ($r = -0,3047$)** y los **días de retraso frente a la fecha prometida ($r = -0,2295$)**. Un pedido de 1 estrella entregado tarde demora **35,65 días** (+11,67 días sobre el límite), más del triple que un pedido de 5 estrellas a tiempo (**10,16 días**). Ni el precio ($r = 0,0036$) ni el valor del flete ($r = -0,0331$) deterioran significativamente la calificación, pero dado que el **67,88 %** de las reseñas de 1 estrella llegaron a tiempo, se concluye que factores de calidad intrínseca del producto o discrepancias con el anuncio explican el resto de la insatisfacción.
* **Respuesta a P3 (Proporción de retrasos por calificación y mes):** En toda la capa Oro, el **7,91 %** de los ítems (`7,77 %` de los que tienen reseña) llega después de la fecha estimada. Esta proporción sube del **2,99 %** en los pedidos de 5 estrellas al **10,34 %** en 3 estrellas, **17,90 %** en 2 estrellas y **32,12 %** en 1 estrella (un incremento de **10,7 veces**). Temporalmente, los retrasos se dispararon en *Black Friday* (noviembre de 2017: **13,83 %**) y en febrero–marzo de 2018 (**15,74 %** y **20,47 %**).
* **Respuesta a P4 (Brecha Sureste vs. Norte y Nordeste):** El **Sureste** concentra el **68,7 %** de los ítems (`75.725`), con entrega media de **10,62 días**, flete de **R\$ 17,34**, retraso de **7,25 %** y calificación de **4,11 ⭐**. El **Norte** (`1,8 %` del volumen) tarda **22,55 días** (+112 %) y paga **R\$ 36,80** de flete (+112 %). El **Nordeste** (`9,2 %` del volumen, `10.087` ítems) tarda **19,85 días**, paga **R\$ 32,26** de flete y sufre la peor tasa de incumplimiento regional (**14,15 %**, casi el doble del Sureste) y la menor calificación (**3,91 ⭐**).
* **Respuesta a P5 (Categorías críticas):** Entre las 45 categorías con $\ge 200$ ítems, **`office_furniture`** presenta la peor calificación (**3,51 ⭐**) y el mayor tiempo de entrega (**20,77 días**), mientras que **`audio`** (**12,57 %** tardía, **3,84 ⭐**) y **`home_confort`** (**10,33 %** tardía, **3,87 ⭐**) lideran los incumplimientos porcentuales.

### 9.2. Limitaciones del dataset y trabajo futuro
1. **Ausencia de desglose por empresa transportadora (*carrier*):** El dataset registra la fecha de entrega al transportista (`order_delivered_carrier_date`), pero no identifica qué operador logístico (Correios vs. transportadoras privadas) realizó el tramo, lo que impide auditar SLA por proveedor.
2. **Corte temporal (2016–2018):** No captura la transformación logística post-2020 ni la introducción del sistema de pagos instantáneos **PIX** en Brasil.
3. **Trabajo futuro:** Incorporar procesamiento de lenguaje natural (NLP) sobre la columna `review_comment_message` (`40.968` comentarios con texto) para separar automáticamente las reseñas de 1 estrella causadas por defectos de producto de aquellas originadas por fallas logísticas, y calcular distancias geodésicas exactas (fórmula de Haversine) cruzando los centroides de `geolocation` entre el código postal del vendedor (`seller_zip_code_prefix`) y el del comprador (`customer_zip_code_prefix`).

### 9.3. Recomendaciones estratégicas para el sector
1. **Recalibración dinámica de promesas de entrega (SLA) por ruta y estacionalidad:** Ajustar el algoritmo que estima `order_estimated_delivery_date` en rutas hacia el **Nordeste (`AL`, `MA`, `SE`, `CE`, `BA`)** y durante eventos de alta demanda (*Black Friday*), eliminando promesas irreales que disparan las reseñas de 1 estrella.
2. **Alianzas de *Fulfillment* regional para mercancía pesada:** Dado que el peso explica el costo del flete ($r = 0,6103$) y que `office_furniture` promedia `20,77` días de tránsito, pre-posicionar inventario de categorías voluminosas en centros de distribución del Nordeste (ej. Salvador o Recife) reducirá simultáneamente tarifas de envío y tiempos de espera.
3. **Sistema proactivo de alertas tempranas en tránsito:** Cuando un pedido supere el umbral de tránsito sin actualización o se acerque a su fecha límite, notificar proactivamente al comprador y otorgar un cupón compensatorio antes de que se emita la encuesta de satisfacción.

---

## 10. Declaración de uso de Inteligencia Artificial (Numeral 4.1)

En cumplimiento del numeral 4.1 de los lineamientos del Proyecto Integrador, el equipo declara que el diseño de la arquitectura de datos, la formulación de las reglas de limpieza, las consultas en PySpark y Spark SQL y la interpretación analítica de los resultados fueron desarrollados y comprendidos íntegramente por los tres integrantes del equipo. Se emplearon herramientas de Inteligencia Artificial generativa exclusivamente como **asistente de apoyo técnico y de interfaz** en las siguientes tareas puntuales:

| Herramienta de IA utilizada | Tarea específica en la que se empleó como apoyo | Qué verificó, ajustó o corrigió el equipo |
| :--- | :--- | :--- |
| **Asistente de IA (ChatGPT / Gemini)** | **Maquetación de la interfaz web en Streamlit (`app/app.py`) y configuración de gráficos en Plotly:** Consulta de sintaxis de componentes visuales de Streamlit (`st.tabs`, `st.columns`, `st.metric`, bloques CSS, `@st.cache_data`) y parámetros estéticos de gráficos en Plotly Express y Graph Objects (doble eje $Y$, escalas de color `RdYlGn` y `RdBu_r`). | El equipo integró manualmente las rutas reales hacia la carpeta `/docs` generada por PySpark, programó la fórmula de ponderación matemática de los KPIs ($\sum (\text{valor} \times \text{ítems}) / \sum \text{ítems}$) para evitar promedios simples sesgados, y validó que todas las cifras mostradas en la web coincidieran exactamente con los `count()` del notebook. |
| **Asistente de IA (ChatGPT / Gemini)** | **Resolución de compatibilidad de entorno local y despliegue web:** Apoyo para estructurar el archivo `.gitignore`, depurar dependencias en `requirements.txt` para el despliegue en *Streamlit Community Cloud* y manejar la excepción de `HADOOP_HOME / winutils.exe` al escribir archivos Parquet localmente en Windows. | Se verificó en ejecución local y en el repositorio que las 9 tablas limpias y `olist_oro` se leyeran y escribieran sin pérdida de esquema ni registros (`110.189` filas intactas) tanto en Google Colab como en Windows. |
| **Asistente de IA (ChatGPT / Gemini)** | **Revisión de estilo y formato de tablas del informe:** Apoyo en la revisión ortotipográfica y formato de tablas comparativas bajo normas APA 7.ª edición. | Todos los valores numéricos, porcentajes, coeficientes de correlación y conclusiones fueron contrastados celda por celda contra las salidas reales ejecutadas en `ProyectoIntegradorBigDataV2.ipynb`. |

---

## 11. Aportes de cada integrante del equipo

**Tabla 6.** *Distribución de responsabilidades y aportes reales por integrante del equipo.*

| Integrante | Rol principal en el proyecto | Tareas ejecutadas y secciones del código / informe bajo su responsabilidad |
| :--- | :--- | :--- |
| **Sebastián Felipe Vargas Quintero** | **Ingeniería de Datos, Ingesta y Limpieza en PySpark (Capas Bronce y Plata)** | • Configuración del entorno PySpark y descarga automatizada reproducible con `kagglehub` (Secciones 0 a 3 del notebook).<br>• Cálculo de volumetría (`1.550.922` filas, `120,35 MB`) y construcción del diccionario de datos de las 52 columnas (Secciones 4 y 5).<br>• Programación del diagnóstico automatizado de calidad por columna (regex de tipos, conteo de nulos, duplicados por clave y atípicos por $IQR$, Sección 6).<br>• Implementación de las reglas de limpieza en PySpark, normalización de texto/ciudades, tabla de métricas antes/después y persistencia en Parquet (Secciones 7 a 9). |
| **Néstor Camilo Peña Guarín** | **Procesamiento Distribuido (Capa Oro), Spark SQL y Despliegue Web en Streamlit** | • Deduplicación de reseñas con funciones de ventana (`Window`) y construcción de la tabla analítica `df_oro` mediante el `join` de 5 tablas (`110.189` ítems, Sección 10.1).<br>• Programación y verificación con `count()` y `show(n)` de las consultas de filtro y `groupBy` por estado (`UF`) y macro-región (Secciones 10.2 y 10.3).<br>• Formulación de las consultas analíticas en **Spark SQL** para cruce de calificaciones vs. cumplimiento y ranking de las 45 categorías (Sección 11).<br>• Desarrollo, expansión a 7 gráficos interactivos de Plotly y despliegue web de la aplicación `app/app.py` en **Streamlit** y configuración del repositorio GitHub (Sección 12). |
| **Luis Carlos Taborda Flórez** | **Análisis Exploratorio, Estadística Descriptiva, Correlación y Evaluación de Negocio** | • Cálculo programático de la tabla de estadística descriptiva (media, desviación estándar, cuartiles $P_{25}, P_{50}, P_{75}, P_{95}$ e $IQR$) con Pandas y NumPy sobre la capa Oro (Sección 13.1).<br>• Construcción y análisis de la matriz de correlación de Pearson entre variables logísticas, económicas y de satisfacción (Sección 13.2).<br>• Elaboración del análisis de evolución temporal mensual (2017–2018), identificación de los picos de *Black Friday* y crisis postal, y análisis financiero de la tabla `payments` (Sección 14).<br>• Generación de visualizaciones estadísticas con Matplotlib/Seaborn (Sección 15) y redacción de conclusiones, limitaciones y recomendaciones para el sector. |

---

## Referencias (Normas APA 7.ª edición)

* Apache Software Foundation. (2024). *Apache Spark: Unified engine for large-scale data processing (PySpark & Spark SQL Documentation)*. https://spark.apache.org/docs/latest/api/python/
* Congreso de la República de Colombia. (2012, 17 de octubre). *Ley Estatutaria 1581 de 2012: Por la cual se dictan disposiciones generales para la protección de datos personales*. Diario Oficial No. 48.587.
* McKinney, W. (2022). *Python for data analysis: Data wrangling with pandas, NumPy, and Jupyter* (3rd ed.). O'Reilly Media.
* Olist. (2018). *Brazilian E-Commerce Public Dataset by Olist* [Conjunto de datos]. Kaggle. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
* Plotly Technologies Inc. (2024). *Plotly Python Open Source Graphing Library Documentation*. https://plotly.com/python/
* Streamlit Inc. (2024). *Streamlit documentation: Faster way to build and share data apps*. https://docs.streamlit.io/
* Universidad Santo Tomás, Seccional Tunja. (2026). *Proyecto integrador: Análisis de datos reales con Big Data en Python [Guía del curso Big Data 2026-2]*. Facultad de Ingeniería de Sistemas.

---

## Anexos

### Anexo A. Código principal de integración de la Capa Oro, Spark SQL y Estadística/Correlación
Fragmentos ejecutados del notebook [ProyectoIntegradorBigDataV2.ipynb](file:///c:/Users/sebastian/Downloads/proyecto_bigdata/notebooks/ProyectoIntegradorBigDataV2.ipynb) (Secciones 10, 11 y 13):

```python
# 10.1. Construcción de la tabla analítica (Join de orders, items, reviews, customers y products)
reviews_unicas = (clean["reviews"].groupBy("order_id")
                  .agg(F.avg("review_score").alias("review_score")))

df_oro = (
    clean["items"]
    .join(clean["orders"], on="order_id", how="inner")
    .join(reviews_unicas, on="order_id", how="left")
    .join(clean["customers"].select("customer_id", "customer_city", "customer_state"),
          on="customer_id", how="inner")
    .join(clean["products"].select("product_id", "product_category_name_english",
                                   "product_weight_g"), on="product_id", how="left")
    .where((F.col("order_status") == "delivered") & (~F.col("fecha_inconsistente")))
    .withColumn("dias_retraso", F.datediff("order_delivered_customer_date",
                                           "order_estimated_delivery_date"))
    .withColumn("valor_total_item", F.round(F.col("price") + F.col("freight_value"), 2))
    .cache()
)
print(f"[Verificación Join Capa Oro] Total de ítems entregados válidos (count): {df_oro.count():,}")

# 11.1. Consulta Spark SQL: Calificación vs. cumplimiento logístico
df_oro.createOrReplaceTempView("olist_oro")
df_sql_reviews = spark.sql("""
    SELECT CAST(ROUND(review_score) AS INT) AS calificacion_estrellas,
           CASE WHEN entrega_tardia THEN 'Con Retraso' ELSE 'A Tiempo' END AS estado_cumplimiento,
           COUNT(*) AS cantidad_items,
           ROUND(AVG(dias_entrega), 2) AS dias_entrega_promedio,
           ROUND(AVG(dias_retraso), 2) AS dias_retraso_promedio,
           ROUND(AVG(freight_value), 2) AS flete_promedio_brl,
           ROUND(AVG(price), 2) AS precio_promedio_brl
    FROM olist_oro
    WHERE review_score IS NOT NULL
    GROUP BY CAST(ROUND(review_score) AS INT),
             CASE WHEN entrega_tardia THEN 'Con Retraso' ELSE 'A Tiempo' END
    ORDER BY calificacion_estrellas ASC, estado_cumplimiento DESC
""")

# 13. Estadística descriptiva y matriz de correlación de Pearson con Pandas y NumPy
pdf_oro = pd.read_parquet(f"{CLEAN}/olist_oro")
num_cols = ["price", "freight_value", "valor_total_item", "product_weight_g",
            "dias_entrega", "dias_retraso", "review_score"]
corr_pearson = pdf_oro[num_cols].corr(method="pearson").round(4)
```

### Anexo B. Diccionario de datos de la Capa Oro y fuentes (extracto principal)
El diccionario completo de las 52 columnas de las 9 tablas con su respectivo responsable de fuente se encuentra exportado en el archivo `docs/diccionario_datos.csv` del repositorio.

| Columna | Tabla de origen | Tipo | Descripción | Ejemplo real del dataset |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | `orders` / `items` / `reviews` | `string` | Identificador único del pedido | `e481f51cbdc54678b7cc49136f2d6af7` |
| `customer_id` | `orders` / `customers` | `string` | Clave del cliente por pedido (seudonimizada MD5) | `06b8999e2fba1a1fbc88172c00ba8bc7` |
| `product_id` | `items` / `products` | `string` | Clave única del producto | `1e9e8ef04dbcff4541ed26657ea517e5` |
| `product_category_name_english` | `products` / `translation` | `string` | Categoría del producto traducida al inglés | `health_beauty` |
| `product_weight_g` | `products` | `integer` | Peso del producto en gramos | `225` |
| `order_status` | `orders` | `string` | Estado logístico del pedido | `delivered` |
| `order_purchase_timestamp` | `orders` | `timestamp` | Fecha y hora de realización de la compra | `2017-10-02 10:56:33` |
| `order_delivered_customer_date` | `orders` | `timestamp` | Fecha y hora de entrega efectiva al cliente | `2017-10-10 21:25:13` |
| `order_estimated_delivery_date` | `orders` | `timestamp` | Fecha estimada de entrega informada al comprador | `2017-10-18 00:00:00` |
| `price` | `items` | `double` | Precio unitario del ítem en reales brasileños (BRL) | `58,90` |
| `freight_value` | `items` | `double` | Costo de flete del ítem en reales brasileños (BRL) | `13,29` |
| `review_score` | `reviews` | `double` | Calificación del cliente de 1 a 5 estrellas | `5,0` |
| `customer_state` | `customers` | `string` | Sigla oficial del estado de destino (`UF`) | `SP` |
| `customer_city` | `customers` | `string` | Ciudad del comprador (normalizada sin tildes) | `franca` |
| `dias_entrega` *(derivada)* | `orders` (Capa Plata) | `integer` | Días transcurridos entre compra y entrega efectiva | `7` |
| `dias_retraso` *(derivada)* | `olist_oro` (Capa Oro) | `integer` | Días entre entrega real y fecha estimada (positivo = tarde) | `-9` |
| `entrega_tardia` *(derivada)* | `orders` (Capa Plata) | `boolean` | `true` si la fecha de entrega superó la fecha estimada | `false` |
| `valor_total_item` *(derivada)* | `olist_oro` (Capa Oro) | `double` | Suma de `price + freight_value` en BRL | `72,19` |

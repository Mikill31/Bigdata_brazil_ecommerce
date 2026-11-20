import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ==============================================================================
# CONFIGURACIÓN DE PÁGINA
# ==============================================================================
st.set_page_config(
    page_title="Olist Big Data Dashboard · USTA Tunja",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos visuales limpios
st.markdown("""
<style>
    .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    div[data-testid="stMetric"] {
        background-color: rgba(240, 242, 246, 0.55);
        border: 1px solid rgba(128, 128, 128, 0.2);
        padding: 12px 16px;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# CARGA DE DATOS (DESDE CAPA ANALÍTICA / DOCS DEL REPOSITORIO)
# ==============================================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCS_DIR = os.path.join(BASE_DIR, "docs")

@st.cache_data
def cargar_datos():
    def leer_csv(nombre, fallback_df):
        ruta = os.path.join(DOCS_DIR, nombre)
        if os.path.exists(ruta):
            return pd.read_csv(ruta)
        return fallback_df

    df_est = leer_csv("olist_resumen_estados.csv", pd.DataFrame({
        "customer_state": ["AP","RR","AM","AL","PA","MA","SE","CE","AC","PB","RO","PI","RN","BA","PE","MT","TO","ES","MS","GO","RJ","RS","SC","DF","PR","MG","SP"],
        "total_items": [81,46,163,427,1054,800,375,1426,91,586,273,523,521,3683,1746,1037,310,2225,802,2277,14143,6134,4097,2355,5649,12916,46448],
        "promedio_dias_entrega": [28.22,28.17,26.34,24.45,23.70,21.59,21.42,20.92,20.68,20.55,19.66,19.32,19.27,19.19,18.22,17.92,17.34,15.65,15.52,15.42,15.12,15.10,14.81,12.89,11.85,11.85,8.66],
        "pct_entrega_tardia": [4.94,10.87,4.29,24.12,12.43,20.38,16.27,15.29,3.30,11.26,4.03,15.49,10.36,13.68,10.19,6.46,11.29,11.82,10.85,7.69,12.90,6.83,9.28,6.75,4.69,5.32,5.39],
        "calificacion_promedio": [4.26,3.89,4.11,3.84,3.84,3.75,3.89,3.88,4.16,4.03,4.08,3.96,4.11,3.87,4.02,4.09,4.14,4.03,4.14,4.06,3.90,4.14,4.09,4.09,4.19,4.15,4.20],
        "flete_promedio_brl": [34.16,43.09,33.31,35.87,35.63,38.49,36.57,32.73,40.05,43.09,41.33,39.12,35.72,26.49,32.69,28.14,37.18,22.02,23.36,22.74,21.02,21.79,21.51,21.06,20.58,20.66,15.17],
        "ventas_totales_brl": [16141.81,9039.52,27585.47,94172.49,212023.57,147803.55,70289.13,266436.77,19575.33,137838.55,56966.0,105178.19,100714.78,591137.81,308972.05,181224.4,60000.5,317682.1,134400.2,334200.1,2085340.2,861290.5,595120.4,346120.5,781450.9,1815620.3,5789410.6]
    }))

    df_rev = leer_csv("olist_resumen_reviews_sql.csv", pd.DataFrame({
        "calificacion_estrellas": [1,1,2,2,3,3,4,4,5,5],
        "estado_cumplimiento": ["Con Retraso","A Tiempo","Con Retraso","A Tiempo","Con Retraso","A Tiempo","Con Retraso","A Tiempo","Con Retraso","A Tiempo"],
        "cantidad_items": [3966,8381,654,2999,946,8205,1019,19978,1877,60804],
        "dias_entrega_promedio": [35.65,11.95,32.69,12.07,29.59,12.17,26.73,11.40,24.32,10.16],
        "dias_retraso_promedio": [11.67,-14.24,9.44,-13.88,7.01,-13.16,5.87,-13.40,4.30,-13.96],
        "flete_promedio_brl": [22.86,20.25,22.09,20.72,22.30,20.07,20.89,20.01,20.89,19.54],
        "precio_promedio_brl": [139.95,115.83,123.51,112.48,125.05,108.00,123.59,118.53,127.14,121.27]
    }))

    df_cat = leer_csv("olist_resumen_categorias_sql.csv", pd.DataFrame())
    df_mes = leer_csv("olist_serie_mensual.csv", pd.DataFrame())
    df_stats = leer_csv("olist_estadistica_descriptiva.csv", pd.DataFrame())
    df_corr = leer_csv("olist_correlacion.csv", pd.DataFrame())
    df_pay = leer_csv("olist_resumen_pagos.csv", pd.DataFrame())
    df_ant_desp = leer_csv("metricas_antes_despues.csv", pd.DataFrame())
    df_vol = leer_csv("volumen.csv", pd.DataFrame())
    df_dic = leer_csv("diccionario_datos.csv", pd.DataFrame())

    # Mapeo de macro-regiones de Brasil
    mapa_region = {
        "SP": "Sudeste", "RJ": "Sudeste", "MG": "Sudeste", "ES": "Sudeste",
        "PR": "Sur", "RS": "Sur", "SC": "Sur",
        "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
        "BA": "Nordeste", "PE": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
        "RN": "Nordeste", "AL": "Nordeste", "PI": "Nordeste", "SE": "Nordeste",
        "PA": "Norte", "AM": "Norte", "RO": "Norte", "TO": "Norte", "AC": "Norte", "AP": "Norte", "RR": "Norte"
    }
    df_est["macro_region"] = df_est["customer_state"].map(mapa_region).fillna("Otra")
    return df_est, df_rev, df_cat, df_mes, df_stats, df_corr, df_pay, df_ant_desp, df_vol, df_dic

df_estados, df_reviews, df_categorias, df_mensual, df_stats, df_corr, df_pagos, df_ant_desp, df_vol, df_dic = cargar_datos()

# ==============================================================================
# BARRA LATERAL: IDENTIFICACIÓN Y FILTROS INTERACTIVOS (MÍNIMO 2 EXIGIDOS, 4 INCLUIDOS)
# ==============================================================================
with st.sidebar:
    st.markdown("## 🎓 Proyecto Integrador")
    st.caption("**Universidad Santo Tomás · Seccional Tunja**\n\nFacultad de Ingeniería de Sistemas · Big Data 2026-2")
    st.markdown("""
    **Curso:** Big Data 2026-2  
    **Docente:** Ing. Martha Susana Contreras Ortiz  

    **Integrantes:**
    * Sebastián Felipe Vargas Quintero
    * Néstor Camilo Peña Guarín
    * Luis Carlos Taborda Flórez
    """)
    st.divider()

    st.header("🔎 Filtros Interactivos")

    regiones_disp = sorted(df_estados["macro_region"].unique())
    regiones_sel = st.multiselect(
        "1. Macro-Región de Brasil:",
        options=regiones_disp,
        default=regiones_disp
    )

    estados_filtrados_por_reg = df_estados[df_estados["macro_region"].isin(regiones_sel)]["customer_state"].unique()
    estados_sel = st.multiselect(
        "2. Estados (UF):",
        options=sorted(estados_filtrados_por_reg),
        default=sorted(estados_filtrados_por_reg)
    )

    max_dias = float(np.ceil(df_estados["promedio_dias_entrega"].max() + 1))
    rango_dias = st.slider(
        "3. Rango de días promedio de entrega (UF):",
        min_value=0.0,
        max_value=max_dias,
        value=(0.0, max_dias),
        step=0.5
    )

    cumplimiento_opts = list(df_reviews["estado_cumplimiento"].unique())
    cumplimiento_sel = st.multiselect(
        "4. Estado de Cumplimiento Logístico:",
        options=cumplimiento_opts,
        default=cumplimiento_opts
    )

    min_items_cat = st.slider(
        "5. Volumen mínimo de ítems por categoría:",
        min_value=200,
        max_value=3000,
        value=200,
        step=100
    )

# Aplicación de filtros
df_est_f = df_estados[
    (df_estados["customer_state"].isin(estados_sel)) &
    (df_estados["promedio_dias_entrega"].between(rango_dias[0], rango_dias[1]))
].copy()

df_rev_f = df_reviews[df_reviews["estado_cumplimiento"].isin(cumplimiento_sel)].copy()

if not df_categorias.empty:
    df_cat_f = df_categorias[df_categorias["total_items"] >= min_items_cat].copy()
else:
    df_cat_f = pd.DataFrame()

# ==============================================================================
# ENCABEZADO PRINCIPAL Y KPIs GLOBALES
# ==============================================================================
st.title("📦 Dashboard Analítico Olist E-Commerce — Big Data 2026-2")
st.markdown(
    "Análisis integral del flujo de datos (**Ingesta → Limpieza PySpark → Capa Oro Spark SQL → Estadística y Visualización**) "
    "sobre **1,550,922 registros crudos** (`120.35 MB`) y **110,189 ítems entregados validados** del comercio electrónico brasileño (Olist)."
)

# Cálculo ponderado de KPIs según filtros activos
total_items_sel = int(df_est_f["total_items"].sum()) if not df_est_f.empty else 0
if total_items_sel > 0:
    dias_pond = (df_est_f["promedio_dias_entrega"] * df_est_f["total_items"]).sum() / total_items_sel
    tardia_pond = (df_est_f["pct_entrega_tardia"] * df_est_f["total_items"]).sum() / total_items_sel
    score_pond = (df_est_f["calificacion_promedio"] * df_est_f["total_items"]).sum() / total_items_sel
    flete_pond = (df_est_f["flete_promedio_brl"] * df_est_f["total_items"]).sum() / total_items_sel
    ventas_sel = df_est_f["ventas_totales_brl"].sum()
else:
    dias_pond = tardia_pond = score_pond = flete_pond = ventas_sel = 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Ítems Entregados (Capa Oro)", f"{total_items_sel:,}", f"{len(df_est_f)} de 27 Estados (UF)")
k2.metric("Tiempo Promedio Entrega", f"{dias_pond:.2f} días", "Mediana global: 10.0 días")
k3.metric("Tasa de Entrega Tardía", f"{tardia_pond:.2f}%", "4,620 casos críticos (score ≤ 2)")
k4.metric("Satisfacción Promedio", f"{score_pond:.2f} ⭐", "r = -0.3047 vs días entrega")
k5.metric("Volumen Ventas (BRL)", f"R$ {ventas_sel:,.0f}", f"Flete prom: R$ {flete_pond:.2f}")

st.divider()

# ==============================================================================
# PESTAÑAS DEL DASHBOARD (6 VISUALIZACIONES DE PLOTLY + TABLAS + CALIDAD)
# ==============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "🌎 1. Desempeño Logístico Regional y Temporal",
    "⭐ 2. Factores de Malas Reseñas y Categorías",
    "📊 3. Estadística Descriptiva, Correlación y Pagos",
    "🏛️ 4. Arquitectura Lakehouse, Calidad y Preguntas T1"
])

# ------------------------------------------------------------------------------
# TAB 1: GEOGRAFÍA Y EVOLUCIÓN TEMPORAL (GRÁFICOS 1 Y 2 DE PLOTLY)
# ------------------------------------------------------------------------------
with tab1:
    st.markdown("### 🚚 Respuesta a Preguntas de Negocio: Desempeño Logístico por Región y Evolución Mensual")
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("Gráfico 1 · Tiempo de Entrega y Satisfacción por Estado (UF)")
        if not df_est_f.empty:
            fig1 = px.bar(
                df_est_f.sort_values("promedio_dias_entrega", ascending=True),
                x="promedio_dias_entrega",
                y="customer_state",
                orientation="h",
                color="calificacion_promedio",
                color_continuous_scale="RdYlGn",
                hover_data=["macro_region", "total_items", "pct_entrega_tardia", "flete_promedio_brl", "ventas_totales_brl"],
                labels={
                    "promedio_dias_entrega": "Días Promedio de Entrega",
                    "customer_state": "Estado (UF)",
                    "calificacion_promedio": "Score ⭐",
                    "macro_region": "Macro-Región",
                    "total_items": "Total Ítems",
                    "pct_entrega_tardia": "% Entrega Tardía",
                    "flete_promedio_brl": "Flete Promedio (BRL)"
                },
                template="plotly_white",
                height=540
            )
            fig1.add_vline(x=12.41, line_dash="dash", line_color="#333333", annotation_text="Media nacional (12.41 d)")
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.warning("Selecciona al menos un Estado (UF) en los filtros de la barra lateral.")

    with col_g2:
        st.subheader("Gráfico 2 · Costo de Flete vs. Tiempo de Entrega por Macro-Región")
        if not df_est_f.empty:
            fig2 = px.scatter(
                df_est_f,
                x="promedio_dias_entrega",
                y="flete_promedio_brl",
                size="total_items",
                color="macro_region",
                text="customer_state",
                hover_name="customer_state",
                hover_data=["pct_entrega_tardia", "calificacion_promedio", "ventas_totales_brl"],
                size_max=48,
                labels={
                    "promedio_dias_entrega": "Días Promedio de Entrega",
                    "flete_promedio_brl": "Flete Promedio (BRL)",
                    "macro_region": "Macro-Región",
                    "total_items": "Volumen de Ítems"
                },
                template="plotly_white",
                height=540
            )
            fig2.update_traces(textposition="top center")
            st.plotly_chart(fig2, use_container_width=True)

    # Gráfico adicional de serie de tiempo mensual
    if not df_mensual.empty:
        st.subheader("Gráfico 3 · Evolución Mensual de Entregas Tardías (%) vs. Calificación Promedio (2017–2018)")
        fig_mes = go.Figure()
        fig_mes.add_trace(go.Bar(
            x=df_mensual["periodo_mes"],
            y=df_mensual["pct_entrega_tardia"],
            name="% Entrega Tardía",
            marker_color="#EF553B",
            opacity=0.82,
            yaxis="y1"
        ))
        fig_mes.add_trace(go.Scatter(
            x=df_mensual["periodo_mes"],
            y=df_mensual["calificacion_promedio"],
            name="Calificación Promedio (⭐)",
            mode="lines+markers",
            line=dict(color="#00CC96", width=3),
            marker=dict(size=8),
            yaxis="y2"
        ))
        fig_mes.update_layout(
            template="plotly_white",
            height=400,
            xaxis_title="Periodo Mensual (Año-Mes)",
            yaxis=dict(title="% de Entregas Tardías", side="left", showgrid=False),
            yaxis2=dict(title="Calificación Promedio (1 a 5 ⭐)", side="right", overlaying="y", range=[3.4, 4.5]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_mes, use_container_width=True)
        st.info(
            "📌 **Hallazgo Temporal Clave:** En **noviembre de 2017 (Black Friday)** y **febrero-marzo de 2018** "
            "(crisis logística y huelga postal), el porcentaje de entregas tardías escaló al **13.83%**, **15.74%** y **20.47%**, "
            "provocando la caída más pronunciada en la satisfacción del cliente (**3.90 ⭐**, **3.78 ⭐** y **3.75 ⭐** respectivamente)."
        )

    with st.expander("📋 Ver Tabla Detallada por Estado (UF) — Salida de PySpark"):
        st.dataframe(df_est_f.sort_values("promedio_dias_entrega", ascending=False), use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 2: RESEÑAS Y CATEGORÍAS (GRÁFICOS 4 Y 5 DE PLOTLY)
# ------------------------------------------------------------------------------
with tab2:
    st.markdown("### ⭐ Respuesta a Preguntas de Negocio: Factores Asociados a las Malas Reseñas y Categorías Críticas")
    col_r1, col_r2 = st.columns(2)

    with col_r1:
        st.subheader("Gráfico 4 · Días de Entrega según Calificación y Cumplimiento")
        if not df_rev_f.empty:
            fig3 = px.bar(
                df_rev_f,
                x="calificacion_estrellas",
                y="dias_entrega_promedio",
                color="estado_cumplimiento",
                barmode="group",
                color_discrete_map={"Con Retraso": "#EF553B", "A Tiempo": "#00CC96"},
                text_auto=".1f",
                hover_data=["cantidad_items", "dias_retraso_promedio", "flete_promedio_brl", "precio_promedio_brl"],
                labels={
                    "calificacion_estrellas": "Calificación de la Reseña (1 a 5 ⭐)",
                    "dias_entrega_promedio": "Días Promedio de Entrega",
                    "estado_cumplimiento": "Cumplimiento Logístico"
                },
                template="plotly_white",
                height=460
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.warning("Selecciona al menos un estado de cumplimiento en la barra lateral.")

    with col_r2:
        st.subheader("Gráfico 5 · Categorías con Menor Satisfacción e Incidencia de Retraso")
        if not df_cat_f.empty:
            top_peores_cat = df_cat_f.sort_values("calificacion_promedio", ascending=True).head(15)
            fig4 = px.bar(
                top_peores_cat.sort_values("calificacion_promedio", ascending=False),
                x="calificacion_promedio",
                y="categoria",
                orientation="h",
                color="pct_entrega_tardia",
                color_continuous_scale="OrRd",
                text_auto=".2f",
                hover_data=["total_items", "promedio_dias_entrega", "ventas_totales_brl"],
                labels={
                    "calificacion_promedio": "Calificación Promedio (⭐)",
                    "categoria": "Categoría de Producto (Inglés)",
                    "pct_entrega_tardia": "% Tardía"
                },
                template="plotly_white",
                height=460
            )
            fig4.update_xaxes(range=[3.0, 4.5])
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No hay categorías con el filtro actual.")

    st.success(
        "📌 **Evidencia Empírica (Spark SQL):** Los pedidos calificados con **1 estrella y entregados con retraso** (`3,966` ítems) "
        "tuvieron un tiempo medio de entrega de **35.65 días** (+11.67 días sobre la fecha estimada), mientras que los pedidos de **5 estrellas "
        "entregados a tiempo** (`60,804` ítems) tardaron apenas **10.16 días** (-13.96 días antes del límite prometido). "
        "En categorías, **`office_furniture`** presenta el peor desempeño (`3.51 ⭐` y `20.77` días promedio de entrega)."
    )

    with st.expander("📋 Ver Tablas de Spark SQL (Reseñas y Categorías)"):
        c_t1, c_t2 = st.columns(2)
        c_t1.dataframe(df_rev_f, use_container_width=True)
        c_t2.dataframe(df_cat_f, use_container_width=True)

# ------------------------------------------------------------------------------
# TAB 3: ESTADÍSTICA DESCRIPTIVA, CORRELACIÓN Y PAGOS (GRÁFICOS 6 Y 7 DE PLOTLY)
# ------------------------------------------------------------------------------
with tab3:
    st.markdown("### 📊 Análisis Exploratorio y Estadístico (Unidad 5: Pandas, NumPy, Correlación)")
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        st.subheader("Gráfico 6 · Matriz de Correlación de Pearson (Capa Oro, n = 110,189)")
        if not df_corr.empty:
            corr_matrix = df_corr.set_index(df_corr.columns[0])
            etiquetas = {
                "price": "Precio (BRL)",
                "freight_value": "Flete (BRL)",
                "valor_total_item": "Valor Total (BRL)",
                "product_weight_g": "Peso Producto (g)",
                "dias_entrega": "Días Entrega",
                "dias_retraso": "Días Retraso",
                "review_score": "Review Score (⭐)"
            }
            corr_Mostrada = corr_matrix.rename(index=etiquetas, columns=etiquetas)
            fig_corr = px.imshow(
                corr_Mostrada,
                text_auto=".3f",
                color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1,
                aspect="auto",
                template="plotly_white",
                height=450
            )
            st.plotly_chart(fig_corr, use_container_width=True)

    with col_s2:
        st.subheader("Gráfico 7 · Distribución del Valor por Método de Pago")
        if not df_pagos.empty:
            fig_pay = px.bar(
                df_pagos,
                x="payment_type",
                y="valor_total_brl",
                color="payment_type",
                text="pct_valor",
                hover_data=["transacciones", "pedidos_unicos", "cuotas_promedio", "valor_promedio_brl"],
                labels={
                    "payment_type": "Método de Pago",
                    "valor_total_brl": "Valor Total Transado (BRL)",
                    "pct_valor": "% del Valor Total"
                },
                template="plotly_white",
                height=450
            )
            fig_pay.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            st.plotly_chart(fig_pay, use_container_width=True)

    st.subheader("📐 Tabla de Estadística Descriptiva (Media, Desviación Estándar, Cuartiles e IQR)")
    if not df_stats.empty:
        st.dataframe(df_stats, use_container_width=True)
        st.markdown("""
        * **Correlaciones principales verificadas (`n = 110,189`):**
          1. **`product_weight_g` vs `freight_value` ($r = 0.6103$):** Correlación positiva moderada-fuerte; el peso del producto explica gran parte del costo del flete junto con la distancia geográfica.
          2. **`dias_entrega` vs `dias_retraso` ($r = 0.5974$):** A mayor tiempo total de tránsito, mayor probabilidad de incumplir la fecha estimada.
          3. **`dias_entrega` vs `review_score` ($r = -0.3047$) y `dias_retraso` vs `review_score` ($r = -0.2295$):** Correlación negativa altamente significativa que demuestra que la demora logística es el principal destructor de satisfacción, muy por encima del precio ($r = 0.0036$) o del costo del flete ($r = -0.0331$).
        """)

# ------------------------------------------------------------------------------
# TAB 4: ARQUITECTURA, CALIDAD DE DATOS Y RESPUESTA A PREGUNTAS T1
# ------------------------------------------------------------------------------
with tab4:
    st.markdown("### 🏛️ Arquitectura Medallion (Electiva B), Calidad de Datos y Respuestas a T1")

    st.markdown(r"""
    #### Síntesis de Respuestas a las 4 Preguntas de Análisis (T1 y T9)
    1. **¿Cuánto tarda la entrega por región y estado (UF) y dónde se concentran los cuellos de botella?**
       * En **São Paulo (SP)** (42.15% del volumen) la entrega tarda en promedio **8.66 días** (5.39% tardía), mientras que en el **Norte y Nordeste** supera los 20 días: **Amapá (28.22 d), Roraima (28.17 d), Amazonas (26.34 d), Alagoas (24.45 d con 24.12% de entregas tardías) y Maranhão (21.59 d con 20.38% tardías)**.
    2. **¿Qué factores logísticos y económicos se asocian a las malas reseñas (1 y 2 estrellas)?**
       * El factor determinante es el **incumplimiento del plazo de entrega** ($r = -0.3047$ con días de entrega y $r = -0.2295$ con días de retraso). Un ítem con retraso y calificación de 1 estrella tarda **35.65 días** en promedio vs. **10.16 días** en pedidos de 5 estrellas a tiempo. El precio del producto ($r = 0.0036$) no deteriora la calificación.
    3. **¿Cuáles categorías de productos presentan mayor vulnerabilidad logística e insatisfacción?**
       * Entre las 45 categorías con $\ge 200$ ítems, las más críticas son **`office_furniture`** (`3.51 ⭐`, `20.77` días de entrega, `8.86%` tardía), **`fixed_telephony`** (`3.76 ⭐`), **`audio`** (`3.84 ⭐`, `12.57%` tardía) y **`home_confort`** (`3.87 ⭐`, `10.33%` tardía).
    4. **¿Cómo evolucionó la operación logística mensualmente y cuál es la estructura financiera de pagos?**
       * Los picos de demanda como **Black Friday (nov-2017: 8,474 ítems, 13.83% tardíos)** y el **colapso postal de feb–mar 2018 (15.74% y 20.47% tardíos)** degradaron la calificación media hasta `3.75 ⭐`. Financieramente, la **tarjeta de crédito domina con el 78.34%** del valor (`R$ 12.54 M`, promedio `3.51` cuotas), seguida por **boleto bancario (17.92%)**.
    """)

    c_q1, c_q2 = st.columns(2)
    with c_q1:
        st.subheader("🧹 Métricas Antes vs. Después de la Limpieza")
        if not df_ant_desp.empty:
            st.dataframe(df_ant_desp, use_container_width=True)
    with c_q2:
        st.subheader("📦 Volumen del Dataset por Tabla (Capa Bronce)")
        if not df_vol.empty:
            st.dataframe(df_vol, use_container_width=True)

    with st.expander("📖 Ver Diccionario de Datos Completo (52 Columnas)"):
        if not df_dic.empty:
            st.dataframe(df_dic, use_container_width=True)

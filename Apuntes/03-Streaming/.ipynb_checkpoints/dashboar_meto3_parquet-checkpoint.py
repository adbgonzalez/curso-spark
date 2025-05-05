import streamlit as st
import pandas as pd
import glob
import os
from datetime import datetime

st.set_page_config(page_title="Weather Aggregated Dashboard", layout="wide")
st.title("🌦️ Dashboard Meteorolóxico en Tempo Real (Parquet)")

# Ruta local onde están os datos descargados de HDFS
DATA_PATH = "/tmp/weather_data"

# Carga dos datos locais Parquet
@st.cache_data(ttl=60)
def load_data():
    files = glob.glob(os.path.join(DATA_PATH, "*.parquet"))
    if not files:
        return pd.DataFrame()

    try:
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"] = pd.to_datetime(df["window_end"])
        return df
    except Exception as e:
        st.error(f"Erro lendo os ficheiros Parquet: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("📭 Sen datos dispoñibles en /tmp/weather_data")
else:
    # Filtros
    cities = sorted(df["city"].dropna().unique())
    city_selected = st.selectbox("Selecciona unha cidade", cities)

    # Conversión a datetime.datetime para o slider
    time_min = df["window_start"].min().to_pydatetime()
    time_max = df["window_end"].max().to_pydatetime()

    time_range = st.slider(
        "Selecciona un rango de tempo",
        min_value=time_min,
        max_value=time_max,
        value=(time_min, time_max),
        format="YYYY-MM-DD HH:mm"
    )

    df_filtered = df[
        (df["city"] == city_selected) &
        (df["window_start"] >= time_range[0]) &
        (df["window_end"] <= time_range[1])
    ].sort_values("window_start")

    st.subheader(f"📈 Evolución de temperatura e vento en {city_selected}")

    if not df_filtered.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.line_chart(df_filtered.set_index("window_start")["avg_temp"], height=300)
        with col2:
            st.line_chart(df_filtered.set_index("window_start")["avg_wind"], height=300)

        st.dataframe(df_filtered)
    else:
        st.info("📉 Non hai datos dispoñibles no rango seleccionado.")

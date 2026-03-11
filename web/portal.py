import os
import shutil
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import Callable, Dict, Union

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

st.set_page_config(
    page_title="IBGE Analytics Portal",
    page_icon=":bar_chart:",
    layout="wide",
)

st.markdown(
    """
    <style>
      .stApp {
        background: radial-gradient(circle at 12% 18%, #121a2b 0%, #0f1726 45%, #0b1220 100%);
      }
      .hero {
        padding: 16px 20px;
        border-radius: 14px;
        background: linear-gradient(120deg, #1b3f63 0%, #2a6a95 55%, #3d86b4 100%);
        color: white;
        margin-bottom: 16px;
        box-shadow: 0 10px 26px rgba(7, 12, 26, 0.45);
      }
      .hero h1 {
        margin: 0;
        font-size: 34px;
      }
      .hero p {
        margin: 6px 0 0 0;
        opacity: 0.9;
      }
      .stApp, .stApp p, .stApp span, .stApp label, .stApp h1, .stApp h2, .stApp h3 {
        color: #e8eef7;
      }
      [data-testid="stMetricLabel"] {
        color: #b8c7db;
      }
      [data-testid="stMetricValue"] {
        color: #f5f9ff;
        font-size: 1.25rem;
      }
      .stTabs [data-baseweb="tab"] {
        color: #c4d2e5;
      }
      .stTabs [aria-selected="true"] {
        color: #f2f7ff;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

INDICATORS: Dict[int, str] = {
    1: "PIB",
    77823: "PIB Per Capita",
    77819: "Gastos Educacao",
    77820: "Gastos Saude",
    77831: "IDH",
    77830: "Expectativa Vida",
    77849: "Habitantes",
}


def apply_pro_chart_style(fig, height: int = 420):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#121c2f",
        font={"family": "Segoe UI, sans-serif", "size": 13, "color": "#e6eef9"},
        margin={"l": 10, "r": 10, "t": 56, "b": 16},
        xaxis_title=None,
        yaxis_title=None,
        height=height,
        showlegend=False,
    )
    fig.update_xaxes(showgrid=True, gridcolor="#2a3a52", zeroline=False, automargin=True)
    fig.update_yaxes(showgrid=False, automargin=True)
    return fig


def get_engine():
    host = os.getenv("PG_IBGE_HOST", "localhost")
    port = os.getenv("PG_IBGE_PORT", "5433")
    db = os.getenv("PG_IBGE_DB", "ibge_data")
    user = os.getenv("PG_IBGE_USER", "ibge_user")
    password = os.getenv("PG_IBGE_PASSWORD", "ibge_password")

    driver_candidates = [
        ("psycopg2", "postgresql+psycopg2"),
        ("psycopg", "postgresql+psycopg"),
        ("pg8000", "postgresql+pg8000"),
    ]

    selected_dialect = None
    for package_name, dialect in driver_candidates:
        if find_spec(package_name) is not None:
            selected_dialect = dialect
            break

    if selected_dialect is None:
        raise RuntimeError(
            "Nenhum driver PostgreSQL encontrado. Instale um driver no mesmo ambiente do Streamlit: "
            "pip install psycopg2-binary"
        )

    return create_engine(f"{selected_dialect}://{user}:{password}@{host}:{port}/{db}")


@st.cache_data(ttl=1800)
def fetch_df(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


@st.cache_data(ttl=1800)
def load_base_data() -> pd.DataFrame:
    ids = ",".join(str(i) for i in INDICATORS.keys())
    return fetch_df(
        f"""
        SELECT
            dp.pais_id,
            dp.pais,
            dp.continente,
            fi.ano,
            fi.indicador_id,
            fi.valor
        FROM analytics_marts.fato_indicador fi
        JOIN analytics_marts.dim_pais dp
            ON fi.pais_id = dp.pais_id
        WHERE fi.indicador_id IN ({ids})
        """
    )


def build_wide_metrics(df: pd.DataFrame) -> pd.DataFrame:
    metric_by_indicator = {
        1: "pib",
        77823: "pib_per_capita",
        77819: "gastos_educacao",
        77820: "gastos_saude",
        77831: "idh",
        77830: "expectativa_vida",
        77849: "habitantes",
    }
    pivot = (
        df.pivot_table(
            index=["pais_id", "pais", "continente", "ano"],
            columns="indicador_id",
            values="valor",
            aggfunc="mean",
        )
        .rename(columns=metric_by_indicator)
        .reset_index()
    )
    for col in metric_by_indicator.values():
        if col not in pivot.columns:
            pivot[col] = pd.NA
    return pivot


def page_header(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="hero">
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_compact_number(value: float, decimals: int = 1, force_million: bool = False) -> str:
    if pd.isna(value):
        return "0"
    abs_value = abs(float(value))
    sign = "-" if float(value) < 0 else ""

    if force_million:
        return f"{sign}{abs_value / 1e6:.{decimals}f}Mi"
    if abs_value >= 1e12:
        return f"{sign}{abs_value / 1e12:.{decimals}f}Tri"
    if abs_value >= 1e9:
        return f"{sign}{abs_value / 1e9:.{decimals}f}Bi"
    if abs_value >= 1e6:
        return f"{sign}{abs_value / 1e6:.{decimals}f}Mi"
    return f"{float(value):,.0f}"


def format_compact_currency(value: float, decimals: int = 1) -> str:
    return f"US$ {format_compact_number(value, decimals=decimals)}"


def run_local_command(cmd: list[str], cwd: Path | None = None) -> tuple[int, str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        shell=False,
    )
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode, output.strip()


def resolve_dbt_executable(project_root: Path) -> str | None:
    dbt_in_path = shutil.which("dbt")
    if dbt_in_path:
        return dbt_in_path

    candidates = [
        project_root / ".venv" / "bin" / "dbt",
        project_root / ".venv-win" / "Scripts" / "dbt.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def with_brazil_in_top15(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    base = df.dropna(subset=[metric_col]).sort_values(metric_col, ascending=False)
    top = base.head(15).copy()
    if top["pais"].astype(str).str.lower().eq("brasil").any():
        return top

    brazil_row = base[base["pais"].astype(str).str.lower().eq("brasil")].head(1)
    if brazil_row.empty:
        return top

    top = pd.concat([top, brazil_row], ignore_index=True)
    top = top.drop_duplicates(subset=["pais"], keep="first")
    if len(top) > 15:
        non_brazil = top[~top["pais"].astype(str).str.lower().eq("brasil")]
        if not non_brazil.empty:
            top = top.drop(non_brazil.sort_values(metric_col, ascending=True).head(1).index)
    return top.sort_values(metric_col, ascending=False)


def build_bar_chart(
    df: pd.DataFrame,
    metric_col: str,
    title: str,
    label_fmt: Union[str, Callable[[float], str]],
):
    data = with_brazil_in_top15(df, metric_col).copy()
    data["destaque"] = data["pais"].astype(str).str.lower().eq("brasil").map({True: "Brasil", False: "Outros"})
    if callable(label_fmt):
        data["label"] = data[metric_col].map(label_fmt)
    else:
        data["label"] = data[metric_col].map(lambda v: label_fmt.format(v=v))

    fig = px.bar(
        data,
        x=metric_col,
        y="pais",
        orientation="h",
        title=title,
        color="destaque",
        color_discrete_map={"Brasil": "#f59e0b", "Outros": "#4d96c8"},
        text="label",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
    fig.update_traces(textposition="outside", cliponaxis=False)
    xmax = float(data[metric_col].max()) if not data.empty else 0.0
    xmin = float(data[metric_col].min()) if not data.empty else 0.0
    left = xmin * 1.25 if xmin < 0 else 0
    right = xmax * 1.25 if xmax > 0 else 1
    fig.update_xaxes(showticklabels=False, range=[left, right])
    fig.update_layout(uniformtext_minsize=10, uniformtext_mode="hide")
    return apply_pro_chart_style(fig, height=max(420, len(data) * 24))


def build_country_year_filled_line(
    df: pd.DataFrame,
    metric_col: str,
    title: str,
    label_fmt: Union[str, Callable[[float], str]],
    selected_country: str,
    label_mode: str = "Rotulos reduzidos (recomendado)",
    text_size: int = 12,
):
    data = df[["ano", metric_col]].dropna(subset=[metric_col]).copy().reset_index(drop=True)
    if data.empty:
        return None

    if callable(label_fmt):
        label_values = data[metric_col].map(label_fmt)
    else:
        label_values = data[metric_col].map(lambda v: label_fmt.format(v=v))
    data["hover_label"] = label_values

    data["label"] = ""
    if label_mode == "Rotulos completos (todos os anos)":
        data["label"] = label_values
    else:
        key_indexes = {len(data) - 1, int(data[metric_col].idxmin()), int(data[metric_col].idxmax())}
        for idx in key_indexes:
            data.loc[idx, "label"] = label_values.loc[idx]

    fig = px.line(data, x="ano", y=metric_col, title=title, markers=True)
    is_brazil = str(selected_country).strip().lower() == "brasil"
    color = "#f59e0b" if is_brazil else "#4d96c8"
    fill_color = "rgba(245, 158, 11, 0.22)" if is_brazil else "rgba(77, 150, 200, 0.22)"
    fig.update_traces(
        line={"color": color, "width": 3},
        marker={"color": color, "size": 8},
        fill="tozeroy",
        fillcolor=fill_color,
        customdata=data[["hover_label"]],
        hovertemplate="Ano: %{x}<br>Valor: %{customdata[0]}<extra></extra>",
        cliponaxis=False,
    )

    label_points = data[data["label"] != ""]
    if not label_points.empty:
        if label_mode == "Rotulos completos (todos os anos)":
            text_positions = ["top center" if i % 2 == 0 else "bottom center" for i in range(len(label_points))]
        else:
            text_positions = "top center"

        fig.add_scatter(
            x=label_points["ano"],
            y=label_points[metric_col],
            mode="text",
            text=label_points["label"],
            textposition=text_positions,
            textfont={"size": text_size, "color": "#e6eef9"},
            showlegend=False,
            hoverinfo="skip",
        )

    ymax = float(data[metric_col].max()) if not data.empty else 0.0
    ymin = float(data[metric_col].min()) if not data.empty else 0.0
    lower = ymin * 1.20 if ymin < 0 else 0
    upper = ymax * 1.22 if ymax > 0 else 1
    fig.update_yaxes(showticklabels=False, range=[lower, upper])
    fig.update_layout(uniformtext_minsize=10, uniformtext_mode="hide", hovermode="x unified")
    return apply_pro_chart_style(fig, height=360)


try:
    project_root = Path(__file__).resolve().parents[1]
    dbt_dir = project_root / "dbt"
    dbt_executable = resolve_dbt_executable(project_root)

    with st.sidebar:
        st.header("Navegacao")
        page = st.radio("Pagina", ["Dados Gerais", "Analise por Pais", "Operacoes"], index=0)

    if page == "Operacoes":
        page_header(
            "Operacoes",
            "Executa carga e transformacoes do pipeline diretamente no portal (ambiente local).",
        )
        st.info("Esta pagina e customizada para este projeto. Streamlit/dbt nao trazem esse painel por padrao.")

        c1, c2 = st.columns(2)
        run_raw = c1.button("Rodar Carga Raw", use_container_width=True)
        run_seed = c2.button("Rodar dbt seed", use_container_width=True)

        c3, c4 = st.columns(2)
        run_dbt = c3.button("Rodar dbt run", use_container_width=True)
        run_test = c4.button("Rodar dbt test", use_container_width=True)

        run_docs = st.button("Rodar dbt docs generate", use_container_width=True)

        if not dbt_executable:
            st.warning(
                "Executavel dbt nao encontrado no ambiente atual. "
                "Ative sua venv correta ou instale dbt-postgres."
            )

        if run_raw:
            with st.spinner("Executando carga raw..."):
                code, output = run_local_command([sys.executable, str(project_root / "src" / "data_loader.py")], cwd=project_root)
            st.code(output or "Sem saida de log.", language="bash")
            if code == 0:
                st.success("Carga raw finalizada com sucesso.")
                st.cache_data.clear()
            else:
                st.error("Falha na carga raw.")

        if run_seed:
            if not dbt_executable:
                st.error("Falha no dbt seed: executavel dbt nao encontrado.")
            else:
                with st.spinner("Executando dbt seed..."):
                    code, output = run_local_command(
                        [dbt_executable, "seed", "--select", "country_continent_depara", "--full-refresh", "--profiles-dir", "."],
                        cwd=dbt_dir,
                    )
                st.code(output or "Sem saida de log.", language="bash")
                if code == 0:
                    st.success("dbt seed concluido com sucesso.")
                    st.cache_data.clear()
                else:
                    st.error("Falha no dbt seed.")

        if run_dbt:
            if not dbt_executable:
                st.error("Falha no dbt run: executavel dbt nao encontrado.")
            else:
                with st.spinner("Executando dbt run..."):
                    code, output = run_local_command([dbt_executable, "run", "--profiles-dir", "."], cwd=dbt_dir)
                st.code(output or "Sem saida de log.", language="bash")
                if code == 0:
                    st.success("dbt run concluido com sucesso.")
                    st.cache_data.clear()
                else:
                    st.error("Falha no dbt run.")

        if run_test:
            if not dbt_executable:
                st.error("Falha no dbt test: executavel dbt nao encontrado.")
            else:
                with st.spinner("Executando dbt test..."):
                    code, output = run_local_command([dbt_executable, "test", "--profiles-dir", "."], cwd=dbt_dir)
                st.code(output or "Sem saida de log.", language="bash")
                if code == 0:
                    st.success("dbt test concluido com sucesso.")
                else:
                    st.error("Falha no dbt test.")

        if run_docs:
            if not dbt_executable:
                st.error("Falha no dbt docs generate: executavel dbt nao encontrado.")
            else:
                with st.spinner("Executando dbt docs generate..."):
                    code, output = run_local_command([dbt_executable, "docs", "generate", "--profiles-dir", "."], cwd=dbt_dir)
                st.code(output or "Sem saida de log.", language="bash")
                if code == 0:
                    st.success("dbt docs generate concluido com sucesso.")
                else:
                    st.error("Falha no dbt docs generate.")

        st.stop()

    base_df = load_base_data()
    if base_df.empty:
        st.warning("Sem dados para exibir no portal.")
        st.stop()

    base_df["pais"] = base_df["pais"].astype(str).str.strip().str.title()
    base_df["continente"] = base_df["continente"].fillna("Nao informado")
    wide_df = build_wide_metrics(base_df)
    year_options = sorted(wide_df["ano"].dropna().astype(int).unique().tolist())

    with st.sidebar:
        selected_year = st.selectbox("Ano", year_options, index=len(year_options) - 1)

    if page == "Dados Gerais":
        page_header(
            "Dados Gerais",
            "Analise consolidada de todos os paises para PIB, PIB per capita, YoY, educacao, saude, expectativa de vida e habitantes.",
        )

        with st.sidebar:
            continent_options = ["Todos"] + sorted(wide_df["continente"].dropna().unique().tolist())
            selected_continent = st.selectbox("Continente", continent_options)

        if selected_continent == "Todos":
            geral_df = wide_df.copy()
        else:
            geral_df = wide_df[wide_df["continente"] == selected_continent].copy()

        latest_df = geral_df[geral_df["ano"] == selected_year].copy()
        if latest_df.empty:
            st.warning("Sem dados para o ano e continente selecionados.")
            st.stop()

        yoy_df = geral_df[["pais", "continente", "ano", "pib"]].dropna(subset=["pib"]).copy()
        yoy_df = yoy_df.sort_values(["pais", "ano"])
        yoy_df["yoy_pib_pct"] = yoy_df.groupby("pais")["pib"].pct_change() * 100
        yoy_latest = yoy_df[yoy_df["ano"] == selected_year].copy()

        latest = {
            "pib_total": latest_df["pib"].sum(),
            "pib_per_capita_medio": latest_df["pib_per_capita"].mean(),
            "yoy_pib_pct": yoy_latest["yoy_pib_pct"].mean(),
            "idh_medio": latest_df["idh"].mean(),
            "habitantes_total": latest_df["habitantes"].sum(),
        }

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("PIB Total (ultimo ano)", format_compact_currency(latest["pib_total"]))
        c2.metric("PIB Per Capita Medio", f"US$ {latest['pib_per_capita_medio']:,.2f}")
        c3.metric("YoY PIB Medio", f"{(latest['yoy_pib_pct'] if pd.notna(latest['yoy_pib_pct']) else 0):.2f}%")
        c4.metric("IDH Medio", f"{(latest['idh_medio'] if pd.notna(latest['idh_medio']) else 0):.3f}")
        c5.metric("Habitantes (total)", format_compact_number(latest["habitantes_total"], force_million=True))

        col1, col2 = st.columns(2)
        fig_pib = build_bar_chart(latest_df.dropna(subset=["pib"]), "pib", f"PIB - Top Paises ({selected_year})", lambda v: format_compact_currency(v))
        fig_pib_pc = build_bar_chart(
            latest_df.dropna(subset=["pib_per_capita"]),
            "pib_per_capita",
            f"PIB Per Capita - Top Paises ({selected_year})",
            lambda v: format_compact_currency(v),
        )
        col1.plotly_chart(fig_pib, use_container_width=True)
        col2.plotly_chart(fig_pib_pc, use_container_width=True)

        col1, col2 = st.columns(2)
        fig_yoy = build_bar_chart(yoy_latest.dropna(subset=["yoy_pib_pct"]), "yoy_pib_pct", f"YoY de PIB (%) - Top Paises ({selected_year})", "{v:.2f}%")
        fig_edu = build_bar_chart(latest_df.dropna(subset=["gastos_educacao"]), "gastos_educacao", f"Gastos com Educacao - Top Paises ({selected_year})", "{v:.2f}%")
        col1.plotly_chart(fig_yoy, use_container_width=True)
        col2.plotly_chart(fig_edu, use_container_width=True)

        col1, col2 = st.columns(2)
        fig_saude = build_bar_chart(latest_df.dropna(subset=["gastos_saude"]), "gastos_saude", f"Gastos com Saude - Top Paises ({selected_year})", "{v:.2f}%")
        fig_vida = build_bar_chart(latest_df.dropna(subset=["expectativa_vida"]), "expectativa_vida", f"Expectativa de Vida - Top Paises ({selected_year})", "{v:.2f}")
        col1.plotly_chart(fig_saude, use_container_width=True)
        col2.plotly_chart(fig_vida, use_container_width=True)

        col1, col2 = st.columns(2)
        fig_idh = build_bar_chart(latest_df.dropna(subset=["idh"]), "idh", f"Indice de Desenvolvimento Humano (IDH) - Top Paises ({selected_year})", "{v:.3f}")
        fig_hab = build_bar_chart(
            latest_df.dropna(subset=["habitantes"]),
            "habitantes",
            f"Habitantes - Top Paises ({selected_year})",
            lambda v: format_compact_number(v, force_million=True),
        )
        col1.plotly_chart(fig_idh, use_container_width=True)
        col2.plotly_chart(fig_hab, use_container_width=True)

    if page == "Analise por Pais":
        page_header(
            "Analise por Pais",
            "Mesmas metricas da visao geral, com foco em um pais e evolucao anual.",
        )

        with st.sidebar:
            continents = ["Todos"] + sorted(wide_df["continente"].dropna().unique().tolist())
            selected_continent = st.selectbox("Continente", continents)
            label_mode = st.radio("Modo de rotulos", ["Rotulos reduzidos (recomendado)", "Rotulos completos (todos os anos)"], index=0)
            label_font = st.slider("Fonte dos rotulos", min_value=8, max_value=13, value=10, step=1)

        if selected_continent == "Todos":
            country_options = sorted(wide_df["pais"].dropna().unique().tolist())
        else:
            country_options = sorted(wide_df[wide_df["continente"] == selected_continent]["pais"].dropna().unique().tolist())

        if not country_options:
            st.warning("Sem paises para o continente selecionado.")
            st.stop()

        default_country = "Brasil" if "Brasil" in country_options else country_options[0]
        selected_country = st.selectbox("Pais", country_options, index=country_options.index(default_country))

        country_df = wide_df[wide_df["pais"] == selected_country].sort_values("ano").copy()
        country_df = country_df[country_df["ano"] <= selected_year].copy()
        if country_df.empty:
            st.warning("Sem dados para o pais selecionado.")
            st.stop()

        country_df["yoy_pib_pct"] = country_df["pib"].pct_change() * 100
        latest = country_df.iloc[-1]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("PIB (ultimo ano)", format_compact_currency(latest["pib"]))
        c2.metric("PIB Per Capita", f"US$ {latest['pib_per_capita']:,.2f}")
        c3.metric("YoY PIB", f"{(latest['yoy_pib_pct'] if pd.notna(latest['yoy_pib_pct']) else 0):.2f}%")
        c4.metric("IDH", f"{(latest['idh'] if pd.notna(latest['idh']) else 0):.3f}")
        c5.metric("Habitantes", format_compact_number(latest["habitantes"], force_million=True))

        for metric_col, title, fmt, text_size in [
            ("pib", f"PIB - {selected_country}", lambda v: format_compact_currency(v), label_font),
            ("pib_per_capita", f"PIB Per Capita - {selected_country}", lambda v: format_compact_currency(v), max(8, label_font - 1)),
            ("yoy_pib_pct", f"YoY do PIB (%) - {selected_country}", "{v:.2f}%", label_font),
            ("gastos_educacao", f"Gastos Educacao (%) - {selected_country}", "{v:.2f}%", label_font),
            ("gastos_saude", f"Gastos Saude (%) - {selected_country}", "{v:.2f}%", label_font),
            ("expectativa_vida", f"Expectativa de Vida - {selected_country}", "{v:.2f}", label_font),
            ("idh", f"IDH - {selected_country}", "{v:.3f}", label_font),
            ("habitantes", f"Habitantes - {selected_country}", lambda v: format_compact_number(v, force_million=True), label_font),
        ]:
            fig_country = build_country_year_filled_line(
                country_df,
                metric_col,
                title,
                fmt,
                selected_country,
                label_mode=label_mode,
                text_size=text_size,
            )
            if fig_country is not None:
                st.plotly_chart(fig_country, use_container_width=True)

except Exception as exc:
    st.error("Falha ao carregar dados do banco.")
    st.info(
        "Se aparecer erro de driver (psycopg2/psycopg/pg8000), rode no mesmo ambiente do Streamlit: "
        "pip install psycopg2-binary"
    )
    st.exception(exc)

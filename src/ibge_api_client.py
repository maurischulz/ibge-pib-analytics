"""Cliente Python para API IBGE com modo de simulacao local.

Quando a API estiver indisponivel, o cliente pode gerar datasets sinteticos
com estrutura compativel para permitir testes end-to-end do pipeline.
"""
import requests
import pandas as pd
from typing import Dict, Any, List, Tuple
from pathlib import Path
import logging
from datetime import datetime
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://servicodados.ibge.gov.br/api/v1"
PIB_TOTAL_INDICATOR_ID = 77827
PIB_PER_CAPITA_INDICATOR_ID = 77823
COUNTRY_INDICATOR_IDS = [77831, 77819, 77820, 77823, 77830, 77840, 77849]
SIM_COUNTRIES = [
    ("BR", "Brasil"),
    ("AR", "Argentina"),
    ("US", "Estados Unidos da America"),
    ("CL", "Chile"),
]


class IBGEAPIClient:
    """Cliente para consumir dados da API pública IBGE."""

    def __init__(self, force_simulation: bool = False):
        self.force_simulation = force_simulation

    @staticmethod
    def fetch_json(url: str) -> Dict[str, Any]:
        """Faz request e retorna JSON."""
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _iter_annual_points(serie_list: List[Dict[str, Any]]) -> List[Tuple[int, float]]:
        points: List[Tuple[int, float]] = []
        for ponto in serie_list:
            if not isinstance(ponto, dict):
                continue
            for periodo, valor in ponto.items():
                if not str(periodo).isdigit() or len(str(periodo)) != 4:
                    continue
                if valor in (None, "..."):
                    continue
                points.append((int(periodo), float(str(valor).replace(",", "."))))
        return points

    @staticmethod
    def fetch_pib_paises() -> pd.DataFrame:
        """
        Busca PIB por pais via API de paises/indicadores.

        Para manter compatibilidade com o pipeline atual, a coluna `trimestre`
        e preenchida com 4 (fim do ano) pois a serie e anual.
        """
        logger.info("Buscando PIB por paises...")
        current_year = datetime.now().year
        periodos = ",".join(str(ano) for ano in range(2012, current_year + 1))
        url = (
            f"{BASE_URL}/paises/-/indicadores/"
            f"{PIB_TOTAL_INDICATOR_ID}|{PIB_PER_CAPITA_INDICATOR_ID}?periodo={periodos}"
        )
        data = IBGEAPIClient.fetch_json(url)

        pib_total: Dict[tuple, float] = {}
        pib_pc: Dict[tuple, float] = {}

        for indicador_obj in data:
            indicador_id = indicador_obj.get("id")
            for serie_obj in indicador_obj.get("series", []):
                pais_nome = serie_obj.get("pais", {}).get("nome")
                for ano, valor_float in IBGEAPIClient._iter_annual_points(serie_obj.get("serie", [])):
                    chave = (pais_nome, ano)
                    if indicador_id == PIB_TOTAL_INDICATOR_ID:
                        pib_total[chave] = valor_float
                    elif indicador_id == PIB_PER_CAPITA_INDICATOR_ID:
                        pib_pc[chave] = valor_float

        records: List[Dict[str, Any]] = []
        for chave, pib in pib_total.items():
            pais_nome, ano = chave
            records.append(
                {
                    "pais": pais_nome,
                    "trimestre": 4,
                    "ano": ano,
                    "pib": pib,
                    "pib_per_capita": pib_pc.get(chave),
                }
            )

        df = pd.DataFrame(records).sort_values(["ano", "pais"], ascending=[False, True])
        logger.info(f"✓ {len(df):,} registros de PIB por pais")
        return df

    @staticmethod
    def fetch_indicadores_paises() -> pd.DataFrame:
        """Busca indicadores anuais por pais para o conjunto solicitado."""
        logger.info("Buscando indicadores por paises...")
        current_year = datetime.now().year
        periodos = ",".join(str(ano) for ano in range(2012, current_year + 1))
        ids = "|".join(str(i) for i in COUNTRY_INDICATOR_IDS)
        url = f"{BASE_URL}/paises/-/indicadores/{ids}?periodo={periodos}"
        data = IBGEAPIClient.fetch_json(url)

        records: List[Dict[str, Any]] = []
        for indicador_obj in data:
            indicador_id = indicador_obj.get("id")
            indicador_nome = indicador_obj.get("indicador")
            unidade = (indicador_obj.get("unidade") or {}).get("id")

            for serie_obj in indicador_obj.get("series", []):
                pais = serie_obj.get("pais", {})
                pais_id = pais.get("id")
                pais_nome = pais.get("nome")

                for ano, valor in IBGEAPIClient._iter_annual_points(serie_obj.get("serie", [])):
                    records.append(
                        {
                            "pais_id": pais_id,
                            "pais": pais_nome,
                            "ano": ano,
                            "indicador_id": indicador_id,
                            "indicador": indicador_nome,
                            "unidade": unidade,
                            "valor": valor,
                        }
                    )

        df = pd.DataFrame(records).sort_values(["ano", "pais", "indicador_id"], ascending=[False, True, True])
        logger.info(f"✓ {len(df):,} registros de indicadores por pais")
        return df

    @staticmethod
    def fetch_ipca_regional() -> pd.DataFrame:
        """
        Busca IPCA por região (inflação mensal).
        
        Indicador IBGE 12468: IPCA (Índice Nacional de Preços ao Consumidor Amplo)
        """
        logger.info("Buscando IPCA (inflacao)...")
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/-24/variaveis/63?localidades=N1[all]"
        data = IBGEAPIClient.fetch_json(url)

        series = data[0]["resultados"][0]["series"][0]["serie"]
        records: List[Dict[str, Any]] = []
        for periodo, valor in series.items():
            if valor in (None, "..."):
                continue
            records.append(
                {
                    "periodo": str(periodo),
                    "valor": float(str(valor).replace(",", ".")),
                }
            )

        df = pd.DataFrame(records)
        logger.info(f"✓ {len(df):,} registros de IPCA")
        return df

    @staticmethod
    def simulated_pib_paises() -> pd.DataFrame:
        """Gera serie anual sintetica de PIB por pais."""
        random.seed(42)
        year = datetime.now().year
        records: List[Dict[str, Any]] = []

        for _, pais in SIM_COUNTRIES:
            base = random.uniform(12_000_000_000, 95_000_000_000)
            pop_factor = random.uniform(8_000, 45_000)
            for ano in range(year - 8, year + 1):
                growth = 1 + (ano - (year - 8)) * 0.015
                noise = random.uniform(0.96, 1.04)
                pib = base * growth * noise
                records.append(
                    {
                        "pais": pais,
                        "trimestre": 4,
                        "ano": ano,
                        "pib": round(pib, 2),
                        "pib_per_capita": round(pib / pop_factor, 2),
                    }
                )

        return pd.DataFrame(records)

    @staticmethod
    def simulated_indicadores_paises() -> pd.DataFrame:
        """Gera serie anual sintetica para indicadores por pais."""
        random.seed(88)
        year = datetime.now().year
        indicator_meta = {
            77831: ("Indicadores sociais - Indice de desenvolvimento humano", "indice"),
            77819: ("Economia - Gastos publicos com educacao", "% do PIB"),
            77820: ("Economia - Gastos publicos com saude", "% do PIB"),
            77823: ("Economia - PIB per capita", "USD"),
            77830: ("Indicadores sociais - Esperanca de vida ao nascer", "anos"),
            77840: ("Meio Ambiente - Areas protegidas no total do territorio nacional", "%"),
            77849: ("Populacao - Populacao", "habitantes"),
        }

        records: List[Dict[str, Any]] = []
        for pais_id, pais in SIM_COUNTRIES:
            for ano in range(year - 8, year + 1):
                for indicador_id in COUNTRY_INDICATOR_IDS:
                    indicador_nome, unidade = indicator_meta[indicador_id]
                    valor = random.uniform(1.0, 100.0)
                    if indicador_id == 77849:
                        valor = random.uniform(2_000_000, 350_000_000)
                    elif indicador_id == 77830:
                        valor = random.uniform(62, 84)
                    elif indicador_id == 77823:
                        valor = random.uniform(2_500, 82_000)

                    records.append(
                        {
                            "pais_id": pais_id,
                            "pais": pais,
                            "ano": ano,
                            "indicador_id": indicador_id,
                            "indicador": indicador_nome,
                            "unidade": unidade,
                            "valor": round(valor, 2),
                        }
                    )

        return pd.DataFrame(records)

    @staticmethod
    def simulated_ipca() -> pd.DataFrame:
        """Gera serie mensal sintetica de IPCA para 24 meses."""
        random.seed(84)
        current = datetime.now()
        records: List[Dict[str, Any]] = []

        for i in range(24, 0, -1):
            month = current.month - i
            year = current.year
            while month <= 0:
                month += 12
                year -= 1
            periodo = f"{year}{month:02d}"
            valor = round(random.uniform(0.1, 0.9), 2)
            records.append({"periodo": periodo, "valor": valor})

        return pd.DataFrame(records)

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Extrai datasets reais; se falhar, cai para simulacao."""
        if self.force_simulation:
            logger.warning("Modo simulacao ativado manualmente.")
            return {
                "ibge_pib_paises.csv": self.simulated_pib_paises(),
                "ibge_indicadores_paises.csv": self.simulated_indicadores_paises(),
                "ibge_ipca.csv": self.simulated_ipca(),
            }

        try:
            pib_df = self.fetch_pib_paises()
            indicadores_df = self.fetch_indicadores_paises()
            ipca_df = self.fetch_ipca_regional()
            return {
                "ibge_pib_paises.csv": pib_df,
                "ibge_indicadores_paises.csv": indicadores_df,
                "ibge_ipca.csv": ipca_df,
            }
        except requests.RequestException as err:
            logger.warning(
                "Falha na API IBGE (%s). Usando dados simulados para validar pipeline.",
                err,
            )
            return {
                "ibge_pib_paises.csv": self.simulated_pib_paises(),
                "ibge_indicadores_paises.csv": self.simulated_indicadores_paises(),
                "ibge_ipca.csv": self.simulated_ipca(),
            }


def save_csv(df: pd.DataFrame, filename: str) -> None:
    """Salva DataFrame em CSV na pasta data/raw/."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    filepath = Path("data/raw") / filename
    df.to_csv(filepath, index=False)
    logger.info(f"  → {filepath}")


if __name__ == "__main__":
    client = IBGEAPIClient()
    datasets = client.extract_all()
    for filename, df in datasets.items():
        save_csv(df, filename)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Carpeta de dags dentro del contenedor
DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))
DATA_FOLDER = os.path.join(DAGS_FOLDER, "data")
CHARTS_FOLDER = os.path.join(DAGS_FOLDER, "charts")

def crear_directorios():
    """Crear directorios necesarios si no existen"""
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(CHARTS_FOLDER, exist_ok=True)
    logging.info(f"Directorios creados: {DATA_FOLDER}, {CHARTS_FOLDER}")

def descargar_datos():
    """Descarga datos de criptomonedas desde CoinGecko API"""
    crear_directorios()
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,  # Aumentado para más datos
        "page": 1,
        "sparkline": "false"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Guardar JSON completo
        json_path = os.path.join(DATA_FOLDER, "crypto_full.json")
        with open(json_path, "w") as f:
            f.write(response.text)
        
        logging.info(f"Datos descargados exitosamente: {len(response.json())} criptomonedas")
        return json_path
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al descargar datos: {e}")
        raise

def procesar_datos():
    """Procesa los datos JSON y crea CSV con campos adicionales"""
    try:
        json_path = os.path.join(DATA_FOLDER, "crypto_full.json")
        csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
        
        # Verificar que el archivo JSON existe
        if not os.path.exists(json_path):
            logging.error(f"Archivo JSON no encontrado: {json_path}")
            raise FileNotFoundError(f"JSON file not found: {json_path}")
        
        with open(json_path, "r", encoding='utf-8') as f:
            data = json.load(f)
        
        # Verificar que hay datos
        if not data or len(data) == 0:
            logging.error("No hay datos en el archivo JSON")
            raise ValueError("No data found in JSON file")
        
        logging.info(f"Procesando {len(data)} criptomonedas...")
        
        # Campos expandidos para análisis completo
        fieldnames = [
            'id', 'symbol', 'name', 'current_price', 'market_cap', 
            'total_volume', 'price_change_percentage_24h', 'market_cap_rank',
            'high_24h', 'low_24h', 'circulating_supply'
        ]
        
        processed_count = 0
        with open(csv_path, "w", newline="", encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for coin in data:
                try:
                    # Función helper para obtener valores seguros
                    def safe_get(key, default_value):
                        value = coin.get(key, default_value)
                        # Convertir None a valor por defecto
                        if value is None:
                            return default_value
                        return value
                    
                    row_data = {
                        'id': safe_get('id', ''),
                        'symbol': str(safe_get('symbol', '')).upper(),
                        'name': safe_get('name', ''),
                        'current_price': safe_get('current_price', 0),
                        'market_cap': safe_get('market_cap', 0),
                        'total_volume': safe_get('total_volume', 0),
                        'price_change_percentage_24h': safe_get('price_change_percentage_24h', 0),
                        'market_cap_rank': safe_get('market_cap_rank', 0),
                        'high_24h': safe_get('high_24h', 0),
                        'low_24h': safe_get('low_24h', 0),
                        'circulating_supply': safe_get('circulating_supply', 0)
                    }
                    
                    writer.writerow(row_data)
                    processed_count += 1
                    
                except Exception as coin_error:
                    logging.warning(f"Error procesando moneda {coin.get('id', 'unknown')}: {coin_error}")
                    continue
        
        logging.info(f"Datos procesados exitosamente: {processed_count} criptomonedas guardadas en {csv_path}")
        
        # Verificar que el archivo CSV se creó correctamente
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Failed to create CSV file: {csv_path}")
        
        # Verificar que el CSV tiene datos
        test_df = pd.read_csv(csv_path)
        if test_df.empty:
            raise ValueError("Created CSV file is empty")
        
        logging.info(f"Verificación exitosa: CSV contiene {len(test_df)} filas")
        return csv_path
        
    except Exception as e:
        logging.error(f"Error crítico en procesar_datos: {e}")
        raise

def analizar_datos():
    """Realiza análisis estadístico de los datos"""
    try:
        csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
        
        # Verificar que el archivo existe
        if not os.path.exists(csv_path):
            logging.error(f"Archivo CSV no encontrado: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Leer datos
        df = pd.read_csv(csv_path)
        logging.info(f"Datos cargados exitosamente: {len(df)} filas")
        
        # Verificar que el DataFrame no esté vacío
        if df.empty:
            logging.error("DataFrame está vacío")
            raise ValueError("DataFrame is empty")
        
        # Convertir columnas numéricas y manejar valores nulos
        numeric_columns = ['current_price', 'market_cap', 'price_change_percentage_24h']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Rellenar NaN con 0 para evitar errores
                df[col] = df[col].fillna(0)
        
        # Análisis estadístico con manejo de errores
        stats = {}
        
        try:
            stats['total_cryptos'] = int(len(df))
            stats['timestamp'] = datetime.now().isoformat()
            
            # Precio máximo
            if 'current_price' in df.columns and not df['current_price'].empty:
                max_price_idx = df['current_price'].idxmax()
                stats['max_price_coin'] = str(df.loc[max_price_idx, 'name'])
                stats['max_price'] = float(df['current_price'].max())
            else:
                stats['max_price_coin'] = "N/A"
                stats['max_price'] = 0.0
            
            # Market cap total
            if 'market_cap' in df.columns:
                stats['total_market_cap'] = float(df['market_cap'].sum())
            else:
                stats['total_market_cap'] = 0.0
            
            # Cambios 24h
            if 'price_change_percentage_24h' in df.columns and not df['price_change_percentage_24h'].empty:
                stats['avg_change_24h'] = float(df['price_change_percentage_24h'].mean())
                
                # Top gainer
                top_gainer_idx = df['price_change_percentage_24h'].idxmax()
                stats['top_gainer'] = str(df.loc[top_gainer_idx, 'name'])
                stats['top_gainer_change'] = float(df['price_change_percentage_24h'].max())
                
                # Top loser
                top_loser_idx = df['price_change_percentage_24h'].idxmin()
                stats['top_loser'] = str(df.loc[top_loser_idx, 'name'])
                stats['top_loser_change'] = float(df['price_change_percentage_24h'].min())
            else:
                stats['avg_change_24h'] = 0.0
                stats['top_gainer'] = "N/A"
                stats['top_gainer_change'] = 0.0
                stats['top_loser'] = "N/A"
                stats['top_loser_change'] = 0.0
                
        except Exception as e:
            logging.error(f"Error en cálculos estadísticos: {e}")
            # Valores por defecto
            stats.update({
                'total_cryptos': len(df) if not df.empty else 0,
                'max_price_coin': "Error",
                'max_price': 0.0,
                'total_market_cap': 0.0,
                'avg_change_24h': 0.0,
                'top_gainer': "Error",
                'top_gainer_change': 0.0,
                'top_loser': "Error",
                'top_loser_change': 0.0,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            })
        
        # Guardar análisis
        analysis_path = os.path.join(DATA_FOLDER, "analysis_summary.json")
        with open(analysis_path, "w", encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        # Log del análisis
        logging.info("="*60)
        logging.info("ANÁLISIS DEL MERCADO DE CRIPTOMONEDAS")
        logging.info("="*60)
        logging.info(f"Total de criptomonedas analizadas: {stats['total_cryptos']}")
        logging.info(f"Criptomoneda con mayor precio: {stats['max_price_coin']} (${stats['max_price']:,.2f})")
        logging.info(f"Market Cap total: ${stats['total_market_cap']:,.0f}")
        logging.info(f"Cambio promedio 24h: {stats['avg_change_24h']:+.2f}%")
        logging.info(f"Mayor ganador: {stats['top_gainer']} ({stats['top_gainer_change']:+.2f}%)")
        logging.info(f"Mayor perdedor: {stats['top_loser']} ({stats['top_loser_change']:+.2f}%)")
        logging.info("="*60)
        
        logging.info(f"Análisis completado y guardado en: {analysis_path}")
        return analysis_path
        
    except Exception as e:
        logging.error(f"Error crítico en analizar_datos: {e}")
        # Crear archivo de error para que el task no falle
        error_stats = {
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'total_cryptos': 0
        }
        analysis_path = os.path.join(DATA_FOLDER, "analysis_summary.json")
        with open(analysis_path, "w") as f:
            json.dump(error_stats, f, indent=2)
        raise

def crear_grafico_precios():
    """Crea gráfico de barras con precios actuales"""
    csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
    df = pd.read_csv(csv_path)
    
    # Top 10 por precio
    top_10 = df.head(10)
    
    plt.figure(figsize=(12, 8))
    colores = plt.cm.viridis(np.linspace(0, 1, len(top_10)))
    
    bars = plt.bar(range(len(top_10)), top_10['current_price'], color=colores)
    plt.title('Precio Actual (USD) - Top 10 Criptomonedas', fontsize=16, fontweight='bold')
    plt.xlabel('Criptomonedas', fontsize=12)
    plt.ylabel('Precio (USD)', fontsize=12)
    plt.xticks(range(len(top_10)), top_10['symbol'], rotation=45)
    plt.grid(True, alpha=0.3)
    
    # Añadir valores en las barras
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'${height:,.2f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    output_path = os.path.join(CHARTS_FOLDER, "01_precios_actuales.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logging.info(f"Gráfico de precios guardado: {output_path}")

def crear_grafico_market_cap():
    """Crea gráfico de pastel con distribución de market cap"""
    csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
    df = pd.read_csv(csv_path)
    
    top_10 = df.head(10)
    otros_market_cap = df[10:]['market_cap'].sum() if len(df) > 10 else 0
    
    if otros_market_cap > 0:
        labels = list(top_10['name']) + ['Otros']
        sizes = list(top_10['market_cap']) + [otros_market_cap]
    else:
        labels = list(top_10['name'])
        sizes = list(top_10['market_cap'])
    
    plt.figure(figsize=(12, 8))
    colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
    
    wedges, texts, autotexts = plt.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                       startangle=90, colors=colors,
                                       textprops={'fontsize': 10})
    plt.title('Distribución de Market Cap - Top 10', fontsize=16, fontweight='bold')
    
    # Mejorar legibilidad
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    plt.tight_layout()
    output_path = os.path.join(CHARTS_FOLDER, "02_market_cap_distribucion.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logging.info(f"Gráfico de market cap guardado: {output_path}")

def crear_grafico_precio_volumen():
    """Crea gráfico de dispersión precio vs volumen"""
    csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
    df = pd.read_csv(csv_path)
    
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(df['total_volume'], df['current_price'], 
                         c=df['price_change_percentage_24h'], 
                         cmap='RdYlGn', alpha=0.7, s=80, edgecolors='black', linewidth=0.5)
    
    plt.title('Precio vs Volumen (Color = Cambio 24h)', fontsize=16, fontweight='bold')
    plt.xlabel('Volumen Total (USD)', fontsize=12)
    plt.ylabel('Precio (USD)', fontsize=12)
    plt.xscale('log')
    plt.yscale('log')
    plt.grid(True, alpha=0.3)
    
    # Colorbar
    cbar = plt.colorbar(scatter)
    cbar.set_label('Cambio 24h (%)', fontsize=12)
    
    # Añadir nombres de las principales
    top_5 = df.head(5)
    for idx, row in top_5.iterrows():
        plt.annotate(row['symbol'], (row['total_volume'], row['current_price']),
                    xytext=(5, 5), textcoords='offset points', fontsize=8,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
    
    plt.tight_layout()
    output_path = os.path.join(CHARTS_FOLDER, "03_precio_vs_volumen.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logging.info(f"Gráfico precio vs volumen guardado: {output_path}")

def crear_grafico_cambios_24h():
    """Crea gráfico de barras horizontales con cambios 24h"""
    csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
    df = pd.read_csv(csv_path)
    
    # Ordenar por cambio porcentual
    df_sorted = df.sort_values('price_change_percentage_24h', ascending=True)
    
    plt.figure(figsize=(12, 10))
    colors = ['red' if x < 0 else 'green' for x in df_sorted['price_change_percentage_24h']]
    
    bars = plt.barh(range(len(df_sorted)), df_sorted['price_change_percentage_24h'], 
                    color=colors, alpha=0.7, edgecolor='black', linewidth=0.5)
    
    plt.title('Cambio Porcentual 24h - Todas las Criptomonedas', fontsize=16, fontweight='bold')
    plt.xlabel('Cambio (%)', fontsize=12)
    plt.ylabel('Criptomonedas', fontsize=12)
    plt.yticks(range(len(df_sorted)), df_sorted['symbol'])
    plt.grid(True, alpha=0.3, axis='x')
    plt.axvline(x=0, color='black', linestyle='-', linewidth=1)
    
    # Añadir valores en las barras
    for i, bar in enumerate(bars):
        width = bar.get_width()
        plt.text(width + (0.2 if width >= 0 else -0.2), bar.get_y() + bar.get_height()/2.,
                f'{width:.1f}%', ha='left' if width >= 0 else 'right', 
                va='center', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    output_path = os.path.join(CHARTS_FOLDER, "04_cambios_24h.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logging.info(f"Gráfico de cambios 24h guardado: {output_path}")

def crear_dashboard_completo():
    """Crea un dashboard combinado con todos los gráficos"""
    csv_path = os.path.join(DATA_FOLDER, "crypto_processed.csv")
    df = pd.read_csv(csv_path)
    
    # Crear figura con 4 subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('Dashboard Completo de Criptomonedas - Top 20', fontsize=20, fontweight='bold')
    
    # 1. Precios actuales (Top 10)
    top_10 = df.head(10)
    colors1 = plt.cm.viridis(np.linspace(0, 1, len(top_10)))
    bars1 = ax1.bar(range(len(top_10)), top_10['current_price'], color=colors1)
    ax1.set_title('Precio Actual (USD) - Top 10', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Criptomonedas')
    ax1.set_ylabel('Precio (USD)')
    ax1.set_xticks(range(len(top_10)))
    ax1.set_xticklabels(top_10['symbol'], rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # 2. Market Cap distribution
    market_caps = top_10['market_cap']
    otros_market_cap = df[10:]['market_cap'].sum() if len(df) > 10 else 0
    if otros_market_cap > 0:
        labels = list(top_10['name']) + ['Otros']
        sizes = list(market_caps) + [otros_market_cap]
    else:
        labels = list(top_10['name'])
        sizes = list(market_caps)
    
    wedges, texts, autotexts = ax2.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                       startangle=90, textprops={'fontsize': 8})
    ax2.set_title('Distribución Market Cap', fontsize=14, fontweight='bold')
    
    # 3. Precio vs Volumen
    scatter = ax3.scatter(df['total_volume'], df['current_price'], 
                         c=df['price_change_percentage_24h'], 
                         cmap='RdYlGn', alpha=0.6, s=60)
    ax3.set_title('Precio vs Volumen', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Volumen Total')
    ax3.set_ylabel('Precio (USD)')
    ax3.set_xscale('log')
    ax3.set_yscale('log')
    ax3.grid(True, alpha=0.3)
    
    # 4. Cambios 24h
    df_sorted = df.sort_values('price_change_percentage_24h', ascending=True)
    colors4 = ['red' if x < 0 else 'green' for x in df_sorted['price_change_percentage_24h']]
    ax4.barh(range(len(df_sorted)), df_sorted['price_change_percentage_24h'], 
             color=colors4, alpha=0.7)
    ax4.set_title('Cambio 24h (%)', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Cambio (%)')
    ax4.set_yticks(range(len(df_sorted)))
    ax4.set_yticklabels(df_sorted['symbol'], fontsize=8)
    ax4.grid(True, alpha=0.3, axis='x')
    ax4.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
    
    plt.tight_layout()
    output_path = os.path.join(CHARTS_FOLDER, "00_dashboard_completo.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logging.info(f"Dashboard completo guardado: {output_path}")

# Configuración del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Definición del DAG
with DAG(
    dag_id='crypto_dashboard_completo',
    default_args=default_args,
    description='Pipeline completo para análisis y visualización de criptomonedas',
    schedule_interval='@daily',  # Ejecutar diariamente
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'analytics', 'dashboard']
) as dag:

    # Tarea 1: Descargar datos
    tarea_descarga = PythonOperator(
        task_id='descargar_datos_coingecko',
        python_callable=descargar_datos,
        doc_md="""
        ## Descarga de Datos
        Descarga información de las top 20 criptomonedas desde CoinGecko API
        """
    )

    # Tarea 2: Procesar datos
    tarea_procesar = PythonOperator(
        task_id='procesar_datos_csv',
        python_callable=procesar_datos,
        doc_md="""
        ## Procesamiento de Datos
        Convierte JSON a CSV con campos adicionales para análisis
        """
    )

    # Tarea 3: Análisis estadístico
    tarea_analizar = PythonOperator(
        task_id='analizar_estadisticas',
        python_callable=analizar_datos,
        doc_md="""
        ## Análisis Estadístico
        Calcula métricas clave del mercado de criptomonedas
        """
    )

    # Tareas de visualización (en paralelo)
    tarea_grafico_precios = PythonOperator(
        task_id='crear_grafico_precios',
        python_callable=crear_grafico_precios
    )

    tarea_grafico_market_cap = PythonOperator(
        task_id='crear_grafico_market_cap',
        python_callable=crear_grafico_market_cap
    )

    tarea_grafico_precio_volumen = PythonOperator(
        task_id='crear_grafico_precio_volumen',
        python_callable=crear_grafico_precio_volumen
    )

    tarea_grafico_cambios = PythonOperator(
        task_id='crear_grafico_cambios_24h',
        python_callable=crear_grafico_cambios_24h
    )

    # Tarea final: Dashboard completo
    tarea_dashboard = PythonOperator(
        task_id='crear_dashboard_completo',
        python_callable=crear_dashboard_completo,
        doc_md="""
        ## Dashboard Completo
        Combina todos los gráficos en un dashboard unificado
        """
    )

    # Definir dependencias
    tarea_descarga >> tarea_procesar >> tarea_analizar

    # Los gráficos individuales se ejecutan en paralelo después del análisis
    tarea_analizar >> [
        tarea_grafico_precios,
        tarea_grafico_market_cap,
        tarea_grafico_precio_volumen,
        tarea_grafico_cambios
    ]

    # El dashboard se crea después de todos los gráficos individuales
    [
        tarea_grafico_precios,
        tarea_grafico_market_cap,
        tarea_grafico_precio_volumen,
        tarea_grafico_cambios
    ] >> tarea_dashboard
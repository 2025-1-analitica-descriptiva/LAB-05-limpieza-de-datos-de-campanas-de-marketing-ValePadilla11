"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import zipfile
import os
from glob import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    os.makedirs("files/output", exist_ok=True)
    
    all_dfs = []
    
    zip_files = glob("files/input/*.zip")
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)
                all_dfs.append(df)
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    client_df = combined_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    
    client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_')
    client_df['education'] = client_df['education'].str.replace('.', '_').replace('unknown', pd.NA)
    client_df['credit_default'] = (client_df['credit_default'] == 'yes').astype(int)
    client_df['mortgage'] = (client_df['mortgage'] == 'yes').astype(int)
    
    campaign_df = combined_df[['client_id', 'number_contacts', 'contact_duration', 
                               'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome',
                               'month', 'day']].copy()
    
    campaign_df['previous_outcome'] = (campaign_df['previous_outcome'] == 'success').astype(int)
    campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'] == 'yes').astype(int)
    
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    
    campaign_df['month_num'] = campaign_df['month'].map(month_map)
    campaign_df['last_contact_date'] = '2022-' + campaign_df['month_num'] + '-' + campaign_df['day'].astype(str).str.zfill(2)
    
    campaign_df = campaign_df[['client_id', 'number_contacts', 'contact_duration', 
                               'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome',
                               'last_contact_date']]
    
    economics_df = combined_df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    
    client_df.to_csv("files/output/client.csv", index=False)
    campaign_df.to_csv("files/output/campaign.csv", index=False)
    economics_df.to_csv("files/output/economics.csv", index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()

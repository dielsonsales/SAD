import math
import pandas as pd

from datetime import datetime
from progress.bar import Bar

# Global constants
SUBMISSION_DATE = 'submission_date'

df = pd.read_csv('B2W-Reviews01.csv', parse_dates=[SUBMISSION_DATE], low_memory=False)

# Remove lines with null values
df.dropna(subset=['product_name'], inplace=True)
df.dropna(subset=['product_brand'], inplace=True)
df.dropna(subset=['site_category_lv1'], inplace=True)
df.dropna(subset=['site_category_lv2'], inplace=True)
df.dropna(subset=['review_title'], inplace=True)
df.dropna(subset=['recommend_to_a_friend'], inplace=True)
df.dropna(subset=['review_text'], inplace=True)
df.dropna(subset=['reviewer_birth_year'], inplace=True)
df.dropna(subset=['reviewer_gender'], inplace=True)
df.dropna(subset=['reviewer_state'], inplace=True)


# Create the data frame estados with the field 'name':
estados_data = []
estados_ids = set()

# Create the data frame tempos with the fields 'id', 'ano', 'dia_ano', 'dia_semana' and 'timestamp':
tempos_data = [] #pd.DataFrame(columns=['id', 'ano', 'dia_ano', 'dia_semana', 'timestamp'])

# Create the data frame usuarios with the fields 'id', 'faixa_etaria' and 'genero':
usuarios_data = [] #pd.DataFrame(columns=['id', 'faixa_etaria', 'genero'])
usuarios_ids = set()
# Create the data frame produtos with the fields 'id', 'nome', 'marca', 'categoria' and 'subcategoria':
produtos_data = [] #pd.DataFrame(columns=['id', 'nome', 'marca', 'categoria', 'subcategoria'])
product_ids = set()
# Create the data frame t_facts with the fields 'id', 'avaliacao', 'recomenda', 'estado_name', 'tempo_id', 'usuario_id' and 'produto_id':
t_facts_data = [] #pd.DataFrame(columns=['id', 'avaliacao', 'recomenda', 'estado_name', 'tempo_id', 'usuario_id', 'produto_id'])


# Separa entre 3 faixas etárias: "desconhecida", "jovem", "adulto" e "idoso".
def calcula_faixa_etaria(birth_year):
  if math.isnan(birth_year):
    return "desconhecida"
  current_year = datetime.now().year
  if 10 <= (current_year - birth_year) <= 30:
    faixa_etaria = "jovem"
  elif 31 <= (current_year - birth_year) <= 50:
    faixa_etaria = "adulto"
  else:
    faixa_etaria = "idoso"
  return faixa_etaria


day_of_week_map = {
    0: 'segunda',
    1: 'terca',
    2: 'quarta',
    3: 'quinta',
    4: 'sexta',
    5: 'sabado',
    6: 'domingo'
}


with Bar("Inserting...", max=len(df)) as bar:
    time_id_counter = 1

    for index, row in df.iterrows():
        reviewer_state = row['reviewer_state']
        if reviewer_state not in estados_ids:
            estados_data.append({'name': reviewer_state})
            estados_ids.add(reviewer_state)

        tempos_data.append({'id': time_id_counter, 'ano': row['submission_date'].year, 'dia_ano': row['submission_date'].dayofyear, 'dia_semana': day_of_week_map[row['submission_date'].weekday()], 'timestamp': row['submission_date']})

        reviewer_id = row['reviewer_id']
        if reviewer_id not in usuarios_ids:
            usuarios_data.append({'id': reviewer_id, 'faixa_etaria': calcula_faixa_etaria(row['reviewer_birth_year']), 'genero': row['reviewer_gender']})
            usuarios_ids.add(reviewer_id)

        product_id = row['product_id']
        if product_id not in product_ids:
            produtos_data.append({'id': product_id, 'nome': row['product_name'], 'marca': row['product_brand'], 'categoria': row['site_category_lv1'], 'subcategoria': row['site_category_lv2']})
            product_ids.add(product_id)

        t_facts_data.append({'id': time_id_counter, 'avaliacao': row['overall_rating'], 'recomenda': row['recommend_to_a_friend'], 'estado_name': reviewer_state, 'tempo_id': time_id_counter, 'usuario_id': row['reviewer_id'], 'produto_id': row['product_id']})

        time_id_counter += 1
        bar.next()


estados = pd.DataFrame(estados_data)
tempos = pd.DataFrame(tempos_data)
usuarios = pd.DataFrame(usuarios_data)
produtos = pd.DataFrame(produtos_data)
t_facts = pd.DataFrame(t_facts_data)

with pd.ExcelWriter('B2W-Reviews01.xlsx') as writer:
    estados.to_excel(writer, sheet_name='Estados', index=False)
    tempos.to_excel(writer, sheet_name='Tempos', index=False)
    usuarios.to_excel(writer, sheet_name='Usuarios', index=False)
    produtos.to_excel(writer, sheet_name='Produtos', index=False)
    t_facts.to_excel(writer, sheet_name='T_Facts', index=False)

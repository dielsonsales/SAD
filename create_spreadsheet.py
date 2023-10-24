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
estados = pd.DataFrame(columns=['name'])

# Create the data frame tempos with the fields 'id', 'ano', 'dia_ano', 'dia_semana' and 'timestamp':
tempos = pd.DataFrame(columns=['id', 'ano', 'dia_ano', 'dia_semana', 'timestamp'])

# Create the data frame usuarios with the fields 'id', 'faixa_etaria' and 'genero':
usuarios = pd.DataFrame(columns=['id', 'faixa_etaria', 'genero'])

# Create the data frame produtos with the fields 'id', 'nome', 'marca', 'categoria' and 'subcategoria':
produtos = pd.DataFrame(columns=['id', 'nome', 'marca', 'categoria', 'subcategoria'])

# Create the data frame t_facts with the fields 'id', 'avaliacao', 'recomenda', 'estado_name', 'tempo_id', 'usuario_id' and 'produto_id':
t_facts = pd.DataFrame(columns=['id', 'avaliacao', 'recomenda', 'estado_name', 'tempo_id', 'usuario_id', 'produto_id'])


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
        if reviewer_state not in estados['name'].values:
            estado = pd.DataFrame({'name': [reviewer_state]})
            estados = estados.append(estado, ignore_index=True)

        tempo = pd.DataFrame({'id': [time_id_counter], 'ano': [row['submission_date'].year], 'dia_ano': [row['submission_date'].dayofyear], 'dia_semana': [day_of_week_map[row['submission_date'].weekday()]], 'timestamp': [row['submission_date']]})
        tempos = tempos.append(tempo, ignore_index=True)

        usuario = usuarios.loc[usuarios['id'] == row['reviewer_id']]
        if usuario.empty:
           usuario = pd.DataFrame({'id': [row['reviewer_id']], 'faixa_etaria': [calcula_faixa_etaria(row['reviewer_birth_year'])], 'genero': [row['reviewer_gender']]})
           usuarios = usuarios.append(usuario, ignore_index=True)
        
        produto = produtos.loc[produtos['id'] == row['product_id']]
        if produto.empty:
            produto = pd.DataFrame({'id': [row['product_id']], 'nome': [row['product_name']], 'marca': [row['product_brand']], 'categoria': [row['site_category_lv1']], 'subcategoria': [row['site_category_lv2']]})
            produtos = produtos.append(produto, ignore_index=True)
        
        t_fact = pd.DataFrame({'id': [time_id_counter], 'avaliacao': [row['overall_rating']], 'recomenda': [row['recommend_to_a_friend']], 'estado_name': [estado['name']], 'tempo_id': [tempo['id']], 'usuario_id': [usuario['id']], 'produto_id': [produto['id']]})
        t_facts = t_facts.append(t_fact, ignore_index=True)

        time_id_counter += 1
        bar.next()




estados.to_excel('B2W-Reviews01.xlsx', sheet_name='Estados', index=False)
tempos.to_excel('B2W-Reviews01.xlsx', sheet_name='Tempos', index=False)
usuarios.to_excel('B2W-Reviews01.xlsx', sheet_name='Usuarios', index=False)
produtos.to_excel('B2W-Reviews01.xlsx', sheet_name='Produtos', index=False)
t_facts.to_excel('B2W-Reviews01.xlsx', sheet_name='T_Facts', index=False)
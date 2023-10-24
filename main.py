import math
import pandas as pd
# import SQLAlchemy

from datetime import datetime
from progress.bar import Bar
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship


# Global constants
SUBMISSION_DATE = 'submission_date'

df = pd.read_csv('B2W-Reviews01.csv', parse_dates=[SUBMISSION_DATE], low_memory=False)

# Remove lines with null state
df.dropna(subset=['reviewer_state'], inplace=True)

engine = create_engine('sqlite:////Users/dielsonsales/Workspace/Python/DataWarehouse/DataWarehouse.db', echo=True)
Base = declarative_base()


# Define as classes das tabelas
class Estado(Base):
    __tablename__ = 'estado'

    # id = Column(Integer, primary_key=True)
    name = Column(String, primary_key=True)


class Tempo(Base):
    __tablename__ = 'tempo'

    id = Column(Integer, primary_key=True)
    ano = Column(Integer)
    dia_ano = Column(Integer)
    dia_semana = Column(Integer)
    timestamp = Column(DateTime)


class Usuario(Base):
    __tablename__ = 'usuario'

    id = Column(String, primary_key=True)
    faixa_etaria = Column(String)
    genero = Column(String)


class Produto(Base):
    __tablename__ = 'produto'

    id = Column(Integer, primary_key=True)
    nome = Column(String)
    marca = Column(String)
    categoria = Column(String)
    subcategoria = Column(String)


class T_FACT(Base):
    __tablename__ = 't_fact'

    id = Column(Integer, primary_key=True)
    avaliacao = Column(Integer)
    recomenda = Column(String)
    estado_name = Column(Integer, ForeignKey('estado.name'))
    tempo_id = Column(Integer, ForeignKey('tempo.id'))
    usuario_id = Column(Integer, ForeignKey('usuario.id'))
    produto_id = Column(Integer, ForeignKey('produto.id'))


Base.metadata.create_all(engine)


# Cria uma sessão com o banco de dados
Session = sessionmaker(bind=engine)
session = Session()


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


with Bar("Inserting...") as bar:
    time_id_counter = 1
    # Itera sobre as linhas do DataFrame e insere os dados nas tabelas
    for index, row in df.iterrows():
        reviewer_state = row['reviewer_state']
        estado = session.query(Estado).filter_by(name=reviewer_state).first()
        if estado is None:
            estado = Estado(name=reviewer_state)

        tempo = Tempo(id=time_id_counter, ano=row['submission_date'].year, dia_ano=row['submission_date'].dayofyear, dia_semana=day_of_week_map[row['submission_date'].weekday()], timestamp=row['submission_date'])
        
        session.commit()

        usuario = session.query(Usuario).filter_by(id=row['reviewer_id']).first()
        if usuario is None:
            usuario = Usuario(id=row['reviewer_id'], faixa_etaria=calcula_faixa_etaria(row['reviewer_birth_year']), genero=row['reviewer_gender'])

        produto = session.query(Produto).filter_by(id=row['product_id']).first()
        if produto is None:
            produto = Produto(id=row['product_id'], nome=row['product_name'], marca=row['product_brand'], categoria=row['site_category_lv1'], subcategoria=row['site_category_lv2'])

        t_fact = T_FACT(avaliacao=row['overall_rating'], recomenda=row['recommend_to_a_friend'], estado_name=estado.name, tempo_id=tempo.id, usuario_id=usuario.id, produto_id=produto.id)

        if reviewer_state and estado:
            session.add(estado)

        session.add(tempo)
        session.add(usuario)
        session.add(produto)
        session.add(t_fact)

        time_id_counter += 1
        bar.next()
   


# time_id_counter = 1

# # Itera sobre as linhas do DataFrame e insere os dados nas tabelas
# for index, row in df.iterrows():
#     reviewer_state = row['reviewer_state']
#     estado = session.query(Estado).filter_by(name=reviewer_state).first()
#     if estado is None:
#       estado = Estado(name=reviewer_state)

#     tempo = Tempo(id=time_id_counter, ano=row['submission_date'].year, dia_ano=row['submission_date'].dayofyear, dia_semana=day_of_week_map[row['submission_date'].weekday()], timestamp=row['submission_date'])
    
#     session.commit()

#     usuario = session.query(Usuario).filter_by(id=row['reviewer_id']).first()
#     if usuario is None:
#       usuario = Usuario(id=row['reviewer_id'], faixa_etaria=calcula_faixa_etaria(row['reviewer_birth_year']), genero=row['reviewer_gender'])

#     produto = session.query(Produto).filter_by(id=row['product_id']).first()
#     if produto is None:
#       produto = Produto(id=row['product_id'], nome=row['product_name'], marca=row['product_brand'], categoria=row['site_category_lv1'], subcategoria=row['site_category_lv2'])

#     t_fact = T_FACT(avaliacao=row['overall_rating'], recomenda=row['recommend_to_a_friend'], estado_name=estado.name, tempo_id=tempo.id, usuario_id=usuario.id, produto_id=produto.id)

#     if reviewer_state and estado:
#       session.add(estado)

#     session.add(tempo)
#     session.add(usuario)
#     session.add(produto)
#     session.add(t_fact)

#     time_id_counter += 1


session.commit()
session.close()


# def main():
#     print("Hello World!")


# if __main__ == "__main__":
#     main()

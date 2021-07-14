from pandas import read_csv, set_option
import numpy as np
import unidecode

# https://queirozf.com/entries/pandas-dataframe-groupby-examples

set_option('display.max_rows', 500)
set_option('display.max_columns', 500)
set_option('display.width', 2000)


def nascimento(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def historico(hist):
    return ' ' if hist == 'nan' else hist


agenda = read_csv('HISTORIC_original.csv', sep=';', encoding='latin-1', low_memory=False)
remover = ['Unnamed: 8', 'CodigoHistorico', 'PosicaoAtributo', 'Unnamed: 9', 'Observacao']
agenda = agenda.drop(remover, axis=1)
agenda = agenda.dropna(subset=['CodPaciente'])
agenda['DataConsulta'] = agenda['DataConsulta'].astype(str).apply(nascimento)
agenda['Historico'] = agenda['Historico'].astype(str).apply(historico)
agenda['CodPaciente'] = agenda[['CodPaciente']].astype(int)
agenda = agenda.dropna(subset=['CodMedico'])
agenda['CodMedico'] = agenda[['CodMedico']].astype(int)
agenda = agenda.groupby(['CodPaciente', 'DataConsulta', 'CodMedico']) # ['Historico'].apply(lambda tags: '##'.join(tags))

# print(agenda.tail())
grupos = list(agenda.groups)

print(grupos[-1])
print(agenda.get_group(grupos[-1])['Historico'].sum())
# agenda.to_csv('agenda_dermacapelli.csv', encoding='UTF8', index=False)

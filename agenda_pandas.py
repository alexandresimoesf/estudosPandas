from pandas import read_csv, set_option, Grouper, DataFrame
import numpy as np
import gerador

set_option('display.max_rows', 500)
set_option('display.max_columns', 500)
set_option('display.width', 2000)

medicos_cod: dict = {20: 203,
                     25: 210,
                     26: 212,
                     2: 211}

especializacao: dict = {203: 776,
                        210: 798,
                        213: 806,
                        212: 804,
                        214: 808,
                        211: 801}


def sql_foreing_key_paciente(key):
    return '(SELECT id FROM public.paciente where paciente.id_paciente_dermacapelli = {} LIMIT 1)'.format(key)


def sql_prontuario_fk(key):
    return '(id from public.paciente where paciente.id_paciente_dermacapelli = {} LIMIT 1)'.format(key)


def quote(informacao):
    return "\'" + informacao + "\'"


def medicos(cod):
    return medicos_cod[cod] if cod in medicos_cod else np.nan


def medicos_especial(cod):
    return especializacao[cod] if cod in especializacao else np.nan


def nascimento(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def historico(hist):
    return ' ' if hist == 'nan' else hist.replace("\'", " ")


def prontuario(key):
    return '(SELECT id FROM public.prontuario WHERE fk_paciente_id = (SELECT id FROM paciente where paciente.id_paciente_dermacapelli={} LIMIT 1) LIMIT 1)'.format(key)


def prontuario_paciente_fk(key):
    return '(SELECT id FROM public.paciente WHERE paciente.id_paciente_dermacapelli = {} LIMIT 1)'.format(key)


def filtro(anamnese_frase: str):
    anamnese_frase = anamnese_frase.replace('<CRLF>', ' ')
    anamnese_frase = anamnese_frase.replace("\'", '')
    return anamnese_frase


agenda = read_csv('HISTORIC.csv', sep=';', encoding='latin-1', low_memory=False)
remover = ['Unnamed: 8', 'CodigoHistorico', 'PosicaoAtributo', 'Unnamed: 9', 'Observacao']
agenda = agenda.drop(remover, axis=1)
agenda = agenda.dropna(subset=['CodPaciente'])
agenda['DataConsulta'] = agenda['DataConsulta'].astype(str).apply(nascimento)
agenda['data_agendada_timestamp'] = agenda['DataConsulta'] + ' 08:00:00'
agenda['horario'] = '08:00:00'
agenda['Historico'] = agenda['Historico'].astype(str).apply(historico)
agenda['descricao'] = ''
agenda['etiqueta'] = 'consulta'
agenda['status'] = 1
agenda['status_consulta'] = 'AGUARDANDO'
agenda['confirm_consulta'] = 'CONFIRMADO'
agenda['codigo_saida'] = 'RETORNO'
agenda['data_solicitacao'] = agenda['DataConsulta']
agenda['data_finalizacao'] = 'null'
agenda['responsavel_recepcao'] = ''
agenda['paciente_online'] = 'false'
agenda['fk_clinica_id'] = 83
agenda['fk_forma_atendimento_id'] = 230
agenda['CodPaciente'] = agenda[['CodPaciente']].astype(int)
agenda = agenda.dropna(subset=['CodMedico'])
agenda['CodMedico'] = agenda[['CodMedico']].astype(int).applymap(medicos)
agenda = agenda.dropna(subset=['CodMedico'])
agenda['fk_especializacao_id'] = agenda[['CodMedico']].astype(int).applymap(medicos_especial)
agenda['CodMedico'] = agenda[['CodMedico']].astype(int)
agenda['fk_paciente_id'] = agenda['CodPaciente'].apply(sql_foreing_key_paciente)

agenda = agenda.rename(columns={'CodPaciente': 'id_paciente_dermacapelli',
                                'DataConsulta': 'data_agendada',
                                'CodMedico': 'fk_medico_id'})

q_agenda = ['data_agendada', 'data_agendada_timestamp', 'horario', 'descricao', 'etiqueta', 'status_consulta', 'confirm_consulta', 'codigo_saida', 'data_solicitacao', 'responsavel_recepcao']


agenda_csv = agenda
agenda_csv = agenda_csv.groupby(['id_paciente_dermacapelli', 'data_agendada'], as_index=False).first()
agenda_csv = agenda_csv.reset_index()
agenda_csv = agenda_csv.drop(['Atributo', 'id_paciente_dermacapelli', 'Historico', 'index'], axis=1)
agenda_csv[q_agenda] = agenda_csv[q_agenda].astype(str).apply(quote)
agenda_csv.to_csv('csv/agenda_dermacapelli.csv', encoding='UTF8', index=False)


prontuario_csv = DataFrame()
prontuario_csv['id_paciente_dermacapelli'] = agenda['id_paciente_dermacapelli'].values
prontuario_csv['id_paciente_dermacapelli'] = prontuario_csv['id_paciente_dermacapelli'].apply(prontuario_paciente_fk)
prontuario_csv['datacriacao'] = '(SELECT now())'
prontuario_csv = prontuario_csv.drop_duplicates(subset=['id_paciente_dermacapelli'])
prontuario_csv = prontuario_csv.rename(columns={'id_paciente_dermacapelli': 'fk_paciente_id'})
prontuario_csv.to_csv('csv/prontuario_dermacapelli.csv', encoding='UTF8', index=False)


prontuarioPermissao_csv = DataFrame()
prontuarioPermissao_csv['id_paciente_dermacapelli'] = agenda['id_paciente_dermacapelli']
prontuarioPermissao_csv['modificado_em'] = '(SELECT now())'
prontuarioPermissao_csv['fk_medico_id'] = agenda['fk_medico_id'].values

prontuarioPermissao_csv['fk_prontuario_id'] = agenda['id_paciente_dermacapelli'].apply(prontuario)
prontuarioPermissao_csv['fk_rede_clinica_id'] = '(SELECT fk_rede_clinica_id FROM public.clinica WHERE id = (83))'
prontuarioPermissao_csv['escrita'] = 'false'
prontuarioPermissao_csv['leitura'] = 'false'

prontuarioPermissao_csv = prontuarioPermissao_csv.groupby(['id_paciente_dermacapelli', 'fk_medico_id']).first()
prontuarioPermissao_csv = prontuarioPermissao_csv.reset_index()
# prontuarioPermissao_csv = prontuarioPermissao_csv.drop_duplicates(subset=['id_paciente_dermacapelli'])
prontuarioPermissao_csv = prontuarioPermissao_csv.drop(['id_paciente_dermacapelli'], axis=1)
prontuarioPermissao_csv.to_csv('csv/prontuarioPermissao_dermacapelli.csv', encoding='UTF8', index=False)


anamnese = agenda.groupby(['id_paciente_dermacapelli', 'data_agendada', 'fk_medico_id'], as_index=False)['Historico'].sum()
anamnese['checksum'] = 'null'
anamnese['fk_prontuario_id'] = anamnese['id_paciente_dermacapelli'].apply(prontuario)
anamnese = anamnese.rename(columns={'Historico': 'anamnese',
                                    'data_agendada': 'datacriacao',
                                    'fk_medico_id': 'fk_responsavel_id'})

anamnese = anamnese.drop(['id_paciente_dermacapelli'], axis=1)
anamnese['anamnese'] = anamnese['anamnese'].astype(str).apply(filtro)
anamnese['anamnese'] = anamnese['anamnese'].apply(quote)
anamnese['datacriacao'] = anamnese['datacriacao'] + ' 08:00:00'
anamnese['datacriacao'] = anamnese['datacriacao'].apply(quote)
anamnese.to_csv('csv/anamnese_dermacapelli.csv', encoding='UTF8', index=False)

gerador.gerar_sql()

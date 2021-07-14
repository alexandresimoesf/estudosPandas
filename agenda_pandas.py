from pandas import read_csv, set_option, Grouper
import numpy as np
import unidecode

# https://queirozf.com/entries/pandas-dataframe-groupby-examples
# INSERT INTO public.agenda(data_agendada, data_agendada_timestamp, horario, descricao, etiqueta, status, status_consulta, confirm_consulta, codigo_saida, data_solicitacao,tipo_atendimento, is_encaixe, data_atendimento, data_finalizacao,paciente_online, fk_clinica_id, fk_medico_id,fk_paciente_id, fk_especializacao_id, fk_forma_atendimento_id) VALUES ('2008-12-08', '2008-12-08 08:00:00', '08:00', '', 'consulta', 1, 'AGUARDANDO', 'CONFIRMADO', 'RETORNO', '2008-12-08', 'CONSULTA', false, '2008-12-08', null, false, 83, 211, (SELECT id FROM public.paciente where paciente.id_paciente_dermacapelli = 1), 801, 230);
# INSERT INTO public.prontuario(datacriacao, fk_paciente_id) SELECT now(), id from public.paciente where paciente.id_paciente_dermacapelli = 1;
# INSERT INTO public.permissao_prontuario_clinica(modificado_em, fk_medico_id, fk_prontuario_id, fk_rede_clinica_id, escrita, leitura) VALUES (now(), 211, (SELECT id FROM public.prontuario WHERE fk_paciente_id = (SELECT id FROM paciente where paciente.id_paciente_dermacapelli=1 LIMIT 1)), (SELECT fk_rede_clinica_id FROM public.clinica WHERE id = (83)), false, false);

set_option('display.max_rows', 500)
set_option('display.max_columns', 500)
set_option('display.width', 2000)

medicos_cod: dict = {20: 203, 25: 210,
                     26: 212, 2: 211}

especializacao: dict = {203: 776, 210: 798,
                        213: 806, 212: 804,
                        214: 808, 211: 801}


def sql_foreing_key(key):
    return '(SELECT id FROM public.paciente where paciente.id_paciente_dermacapelli = {})'.format(key)


def sql_prontuario_fk(key):
    return '(id from public.paciente where paciente.id_paciente_dermacapelli = {})'.format(key)


def quote(informacao):
    return '\"' + informacao + '\"'


def medicos(cod):
    return medicos_cod[cod] if cod in medicos_cod else np.nan


def medicos_especial(cod):
    return especializacao[cod] if cod in especializacao else np.nan


def nascimento(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def historico(hist):
    return ' ' if hist == 'nan' else hist


agenda = read_csv('HISTORIC_original.csv', sep=';', encoding='latin-1', low_memory=False)
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
agenda['cod_saida'] = 'RETORNO'
agenda['data_solicitacao'] = agenda['DataConsulta']
agenda['data_finalizacao'] = 'null'
agenda['responsavel_recepcao'] = ''
agenda['paciente_online'] = 'false'
agenda['fk_clinica_id'] = 83
agenda['fk_forma_atendimento'] = 230
agenda['CodPaciente'] = agenda[['CodPaciente']].astype(int)
agenda = agenda.dropna(subset=['CodMedico'])
agenda['CodMedico'] = agenda[['CodMedico']].astype(int).applymap(medicos)
agenda = agenda.dropna(subset=['CodMedico'])
agenda['fk_especializacao_id'] = agenda[['CodMedico']].astype(int).applymap(medicos_especial)
agenda['CodMedico'] = agenda[['CodMedico']].astype(int)
agenda['fk_paciente_id'] = agenda['CodPaciente'].apply(sql_foreing_key)

agenda = agenda.rename(columns={'CodPaciente': 'id_paciente_dermacapelli',
                                'DataConsulta': 'data_agendada',
                                'CodMedico': 'fk_medico_id'})

q = ['data_agendada', 'data_agendada_timestamp', 'horario', 'descricao', 'etiqueta', 'status_consulta', 'confirm_consulta', 'cod_saida', 'data_solicitacao', 'responsavel_recepcao']


# agenda_csv = agenda
# agenda_csv = agenda_csv.drop(['Atributo', 'id_paciente_dermacapelli', 'Historico'], axis=1)
# agenda_csv = agenda_csv.drop_duplicates(subset=['data_agendada'])
# agenda_csv[q] = agenda_csv[q].astype(str).apply(quote)
# agenda_csv.to_csv('agenda_dermacapelli.csv', encoding='UTF8', index=False)
#
#
prontuario_csv = agenda['id_paciente_dermacapelli']
prontuario_csv['datacriacao'] = 'SELECT now()'
prontuario_csv = prontuario_csv.drop_duplicates(subset=['id_paciente_dermacapelli'])
prontuario_csv.to_csv('prontuario_dermacappelli.csv', encoding='UTF8', index=False)


# anamnese = agenda.groupby(['id_paciente_dermacapelli', 'data_agendada', 'fk_medico_id'], as_index=False)['Historico'].sum()

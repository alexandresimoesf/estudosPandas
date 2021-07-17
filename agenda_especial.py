from pandas import read_csv, set_option, DataFrame

# select * from medico where id in (203, 210, 211, 212)

set_option('display.max_rows', 500)
set_option('display.max_columns', 500)
set_option('display.width', 2000)



def prontuario_paciente_fk(key):
    return '(SELECT id FROM public.paciente WHERE paciente.id_paciente_dermacapelli = {})'.format(key)


def sql_foreing_key_paciente(key):
    return '(SELECT id FROM public.paciente where paciente.id_paciente_dermacapelli = {})'.format(key)


def formatar_data(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def quote(informacao):
    return "\'" + informacao + "\'"


def horario(h):
    return '08:00' if 'M' in h or 'T' in h else h


def prontuario(key):
    return '(SELECT id FROM public.prontuario WHERE fk_paciente_id = (SELECT id FROM paciente where paciente.id_paciente_dermacapelli={} LIMIT 1))'.format(key)



agenda_do_medico = read_csv('SOFIA.csv', sep=';', encoding='latin-1', low_memory=False)

usando = ['Data', 'Hora', 'CodPaciente']
n_usando = ['Data', 'Hora', 'NomePaciente', 'Atendente', 'PacienteConfirmado',
       'TemObs', 'Convenio', 'TemFicha', 'Tipo', 'HoraChegada', 'CodPaciente',
       'Efetivada', 'Telefone', 'UsrMarcacao', 'DtMarcacao', 'HoraMarcacao',
       'UsrConfirmacao', 'DtConfirmacao', 'HoraConfirmacao',
       'DtUltimaAlteracao', 'HoraUltimaAlteracao', 'CodAgendamento',
       'NomeAgenda', 'EnviarSMS', 'EnviarEmail', 'EMail', 'Telefone2',
       'Filler', 'Observacao']

agenda_do_medico = agenda_do_medico.drop(list(set(n_usando) - set(usando)), axis=1)
agenda_do_medico['descricao'] = "''"
agenda_do_medico['etiqueta'] = "'consulta'"
agenda_do_medico['status'] = 1
agenda_do_medico['status_consulta'] = "'AGUARDANDO'"
agenda_do_medico['confirm_consulta'] = "'CONFIRMADO'"
agenda_do_medico['codigo_saida'] = "'RETORNO'"
agenda_do_medico['data_finalizacao'] = 'null'
agenda_do_medico['responsavel_recepcao'] = "''"
agenda_do_medico['paciente_online'] = 'false'
agenda_do_medico['fk_clinica_id'] = 83
agenda_do_medico['fk_especializacao_id'] = 798
agenda_do_medico['fk_forma_atendimento_id'] = 230
agenda_do_medico['fk_medico_id'] = 210
agenda_do_medico = agenda_do_medico.dropna(subset=['Data', 'CodPaciente'])
agenda_do_medico['Data'] = agenda_do_medico['Data'].astype(str).apply(formatar_data)
agenda_do_medico['CodPaciente'] = agenda_do_medico['CodPaciente'].astype(int)
agenda_do_medico['data_agendada'] = agenda_do_medico['Data'].astype(str)
agenda_do_medico['data_solicitacao'] = agenda_do_medico['data_agendada']
agenda_do_medico['horario'] = agenda_do_medico['Hora'].astype(str) + ':00'
agenda_do_medico['Hora'] = agenda_do_medico['Hora'].astype(str).apply(horario)
agenda_do_medico['data_agendada_timestamp'] = agenda_do_medico['data_agendada'] + ' ' + agenda_do_medico['Hora'].astype(str) + ':00'
agenda_do_medico = agenda_do_medico.drop(['Data', 'Hora'], axis=1)
agenda_do_medico = agenda_do_medico.rename(columns={'CodPaciente': 'fk_paciente_id'})
agenda_do_medico['fk_paciente_id'] = agenda_do_medico['fk_paciente_id'].apply(sql_foreing_key_paciente)
agenda_do_medico[['horario', 'data_agendada', 'data_agendada_timestamp', 'data_solicitacao']] = agenda_do_medico[['horario', 'data_agendada', 'data_agendada_timestamp', 'data_solicitacao']].apply(quote)
agenda_do_medico.to_csv('csv/sofia_agenda_dermacapelli.csv', index=False)


prontuario_csv = DataFrame()
prontuario_csv['fk_paciente_id'] = agenda_do_medico['fk_paciente_id'].values
prontuario_csv['fk_paciente_id'] = prontuario_csv['fk_paciente_id'].apply(prontuario_paciente_fk)
prontuario_csv['datacriacao'] = '(SELECT now())'
prontuario_csv = prontuario_csv.drop_duplicates(subset=['fk_paciente_id'])
prontuario_csv.to_csv('csv/sofia_prontuario_dermacapelli.csv', encoding='UTF8', index=False)


prontuarioPermissao_csv = DataFrame()
prontuarioPermissao_csv['id_paciente_dermacapelli'] = agenda_do_medico['fk_paciente_id']
prontuarioPermissao_csv['modificado_em'] = '(SELECT now())'
prontuarioPermissao_csv['fk_medico_id'] = agenda_do_medico['fk_medico_id'].values

prontuarioPermissao_csv['fk_prontuario_id'] = agenda_do_medico['fk_paciente_id'].apply(prontuario)
prontuarioPermissao_csv['fk_rede_clinica_id'] = '(SELECT fk_rede_clinica_id FROM public.clinica WHERE id = (83))'
prontuarioPermissao_csv['escrita'] = 'false'
prontuarioPermissao_csv['leitura'] = 'false'

prontuarioPermissao_csv = prontuarioPermissao_csv.groupby(['id_paciente_dermacapelli', 'fk_medico_id']).first()
prontuarioPermissao_csv = prontuarioPermissao_csv.reset_index()
prontuarioPermissao_csv = prontuarioPermissao_csv.drop(['id_paciente_dermacapelli'], axis=1)
prontuarioPermissao_csv.to_csv('csv/sofia_prontuarioPermissao_dermacapelli.csv', encoding='UTF8', index=False)
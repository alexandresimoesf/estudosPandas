from pandas import read_csv, set_option


set_option('display.max_rows', 500)
set_option('display.max_columns', 500)
set_option('display.width', 2000)


def sql_foreing_key_paciente(key):
    return '(SELECT id FROM public.paciente where paciente.id_paciente_dermacapelli = {})'.format(key)


def formatar_data(data):
    return '2020/01/01' if '_' in data else '{}{}{}{}-{}{}-{}{}'.format(*data)


def quote(informacao):
    return "\'" + informacao + "\'"


def horario(h):
    return '08:00' if 'M' in h or 'T' in h else h


agenda_do_medico = read_csv('LEONARDO.csv', sep=';', encoding='latin-1', low_memory=False)

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
agenda_do_medico['responsavel_recepcao'] = ''
agenda_do_medico['paciente_online'] = 'false'
agenda_do_medico['fk_clinica_id'] = 83
agenda_do_medico['fk_especializacao_id'] = 776
agenda_do_medico['fk_forma_atendimento_id'] = 230
agenda_do_medico['fk_medico_id'] = 203
agenda_do_medico = agenda_do_medico.dropna(subset=['Data', 'CodPaciente'])
agenda_do_medico['Data'] = agenda_do_medico['Data'].astype(str).apply(formatar_data)
agenda_do_medico['CodPaciente'] = agenda_do_medico['CodPaciente'].astype(int)
agenda_do_medico['data_agendada'] = agenda_do_medico['Data'].astype(str)
agenda_do_medico['data_solicitacao'] = agenda_do_medico['data_agendada']
agenda_do_medico['horario'] = agenda_do_medico['Hora'].astype(str) + ':00'
agenda_do_medico['Hora'] = agenda_do_medico['Hora'].astype(str).apply(horario)
agenda_do_medico['data_agendada_timestamp'] = agenda_do_medico['data_agendada'] + ' ' + agenda_do_medico['Hora'].astype(str) + ':00'
agenda_do_medico = agenda_do_medico.drop(['Data', 'Hora'], axis=1)
agenda_do_medico = agenda_do_medico.rename(columns={'CodPaciente':'fk_paciente_id'})
agenda_do_medico['fk_paciente_id'] = agenda_do_medico['fk_paciente_id'].apply(sql_foreing_key_paciente)
agenda_do_medico[['horario', 'data_agendada', 'data_agendada_timestamp', 'data_solicitacao']] = agenda_do_medico[['horario', 'data_agendada', 'data_agendada_timestamp', 'data_solicitacao']].apply(quote)
agenda_do_medico.to_csv('csv/leonardo_agenda.csv', index=False)

